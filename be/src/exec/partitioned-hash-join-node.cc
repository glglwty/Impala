// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "exec/partitioned-hash-join-node.h"

#include <sstream>

#include "exec/hash-table.inline.h"
#include "exprs/expr.h"
#include "runtime/buffered-block-mgr.h"
#include "runtime/buffered-tuple-stream.inline.h"
#include "runtime/row-batch.h"
#include "runtime/runtime-state.h"
#include "util/debug-util.h"
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

using namespace boost;
using namespace impala;
using namespace std;

// Number of initial partitions to create. Must be a power of two.
// TODO: this is set to a lower than actual value for testing.
static const int PARTITION_FANOUT = 4;

// Needs to be the log(PARTITION_FANOUT)
static const int NUM_PARTITIONING_BITS = 2;

// Maximum number of times we will repartition. The maximum build table we
// can process is:
// MEM_LIMIT * (PARTITION_FANOUT ^ MAX_PARTITION_DEPTH). With a (low) 1GB
// limit and 64 fanout, we can support 256TB build tables in the case where
// there is no skew.
// In the case where there is skew, repartitioning is unlikely to help (assuming a
// reasonable hash function).
// TODO: we can revisit and try harder to explicitly detect skew.
static const int MAX_PARTITION_DEPTH = 3;

// Maximum number of build tables that can be in memory at any time. This is in
// addition to the memory constraints and is used for testing to trigger code paths
// for small tables.
// TODO: remove
static const int MAX_IN_MEM_BUILD_TABLES = PARTITION_FANOUT / 2;

PartitionedHashJoinNode::PartitionedHashJoinNode(
    ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs)
  : BlockingJoinNode("PartitionedHashJoinNode", tnode.hash_join_node.join_op,
        pool, tnode, descs),
    state_(PARTITIONING_BUILD),
    input_partition_(NULL) {
}

PartitionedHashJoinNode::~PartitionedHashJoinNode() {
}

Status PartitionedHashJoinNode::Init(const TPlanNode& tnode) {
  RETURN_IF_ERROR(BlockingJoinNode::Init(tnode));
  if (join_op_ != TJoinOp::INNER_JOIN) {
    return Status("Only inner join is implemented for partitioned hash joins.");
  }

  DCHECK(tnode.__isset.hash_join_node);
  const vector<TEqJoinCondition>& eq_join_conjuncts =
      tnode.hash_join_node.eq_join_conjuncts;
  for (int i = 0; i < eq_join_conjuncts.size(); ++i) {
    Expr* expr;
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].left, &expr));
    probe_exprs_.push_back(expr);
    RETURN_IF_ERROR(Expr::CreateExprTree(pool_, eq_join_conjuncts[i].right, &expr));
    build_exprs_.push_back(expr);
  }
  RETURN_IF_ERROR(
      Expr::CreateExprTrees(pool_, tnode.hash_join_node.other_join_conjuncts,
                            &other_join_conjuncts_));
  return Status::OK;
}

Status PartitionedHashJoinNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(BlockingJoinNode::Prepare(state));
  // Use 80% of what is left. TODO: this is just for testing.
  int64_t max_join_mem = mem_tracker()->SpareCapacity() * 0.8f;
  if (state->query_options().max_join_memory > 0) {
    max_join_mem = state->query_options().max_join_memory;
  }
  LOG(ERROR) << "Max join memory: "
             << PrettyPrinter::Print(max_join_mem, TCounterType::BYTES);
  join_node_mem_tracker_.reset(new MemTracker(
      state->query_options().max_join_memory, -1, "Hash Join Mem Limit", mem_tracker()));

  // build and probe exprs are evaluated in the context of the rows produced by our
  // right and left children, respectively
  RETURN_IF_ERROR(Expr::Prepare(build_exprs_, state, child(1)->row_desc(), false));
  RETURN_IF_ERROR(Expr::Prepare(probe_exprs_, state, child(0)->row_desc(), false));

  // other_join_conjuncts_ are evaluated in the context of the rows produced by this node
  RETURN_IF_ERROR(Expr::Prepare(other_join_conjuncts_, state, row_descriptor_, false));

  RETURN_IF_ERROR(state->CreateBlockMgr(join_node_mem_tracker()->SpareCapacity()));
  // We need one output buffer per partition and one additional buffer either for the
  // input (while repartitioning) or to contain the hash table.
  int num_reserved_buffers = PARTITION_FANOUT + 1;
  RETURN_IF_ERROR(state->block_mgr()->RegisterClient(
      num_reserved_buffers, join_node_mem_tracker(), &client_));

  // Construct the dummy hash table used to evaluate hashes of rows.
  // TODO: this is obviously not the right abstraction. We need a Hash utility class of
  // some kind.
  hash_tbl_.reset(new HashTable(state, build_exprs_, probe_exprs_,
      child(1)->row_desc().tuple_descriptors().size(), false, false,
      state->fragment_hash_seed(), mem_tracker()));
  return Status::OK;
}

void PartitionedHashJoinNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    hash_partitions_[i]->Close();
  }
  for (list<Partition*>::iterator it = spilled_partitions_.begin();
      it != spilled_partitions_.end(); ++it) {
    (*it)->Close();
  }
  if (input_partition_ != NULL) input_partition_->Close();
  Expr::Close(build_exprs_, state);
  Expr::Close(probe_exprs_, state);
  Expr::Close(other_join_conjuncts_, state);
  BlockingJoinNode::Close(state);
}

PartitionedHashJoinNode::Partition::Partition(RuntimeState* state,
        PartitionedHashJoinNode* parent, int level)
  : parent_(parent),
    is_closed_(false),
    level_(level),
    build_rows_(new BufferedTupleStream(
        state, parent_->child(1)->row_desc(), state->block_mgr(), parent_->client_)),
    probe_rows_(new BufferedTupleStream(
        state, parent_->child(0)->row_desc(), state->block_mgr(), parent_->client_)) {
}

PartitionedHashJoinNode::Partition::~Partition() {
  DCHECK(is_closed());
}

int64_t PartitionedHashJoinNode::Partition::EstimatedInMemSize() const {
  return build_rows_->byte_size() + HashTable::EstimateSize(build_rows_->num_rows());
}

int64_t PartitionedHashJoinNode::Partition::InMemSize() const {
  DCHECK(hash_tbl_.get() != NULL);
  return build_rows_->byte_size() + hash_tbl_->byte_size();
}

void PartitionedHashJoinNode::Partition::Close() {
  if (is_closed()) return;
  is_closed_ = true;
  if (hash_tbl_.get() != NULL) hash_tbl_->Close();
  if (build_rows_.get() != NULL) build_rows_->Close();
  if (probe_rows_.get() != NULL) probe_rows_->Close();
}

Status PartitionedHashJoinNode::Partition::BuildHashTable(
    RuntimeState* state, bool* built) {
  DCHECK(build_rows_.get() != NULL);
  *built = false;
  // First pin the entire build stream in memory.
  RETURN_IF_ERROR(build_rows_->PrepareForRead(true, built));
  if (!*built) return Status::OK;

  hash_tbl_.reset(new HashTable(state, parent_->build_exprs_, parent_->probe_exprs_,
      parent_->child(1)->row_desc().tuple_descriptors().size(),
      false, false, state->fragment_hash_seed(), parent_->join_node_mem_tracker()));

  bool eos = false;
  RowBatch batch(parent_->child(1)->row_desc(), state->batch_size(),
      parent_->mem_tracker());
  while (!eos) {
    RETURN_IF_ERROR(build_rows_->GetNext(&batch, &eos));
    for (int i = 0; i < batch.num_rows(); ++i) {
      hash_tbl_->Insert(batch.GetRow(i));
    }
    parent_->build_pool_->AcquireData(batch.tuple_data_pool(), false);
    batch.Reset();

    if (hash_tbl_->mem_limit_exceeded()) {
      hash_tbl_->Close();
      RETURN_IF_ERROR(build_rows_->Unpin());
      *built = false;
      return Status::OK;
    }
  }
  return Status::OK;
}

// TODO: can we do better with the spilling heuristic.
Status PartitionedHashJoinNode::SpillPartitions() {
  int64_t max_freed_mem = 0;
  int partition_idx = -1;

  // Iterate over the partitions and pick a partition that is already spilled.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->hash_tbl() != NULL) continue;
    int64_t mem = hash_partitions_[i]->build_rows()->bytes_in_mem(true);
    if (mem > max_freed_mem) {
      max_freed_mem = mem;
      partition_idx = i;
    }
  }

  if (partition_idx == -1) {
    // Could not find a partition to spill. This means the mem limit was just too
    // low. e.g. mem_limit too small that we can't put a buffer in front of each
    // partition.
    Status status = Status::MEM_LIMIT_EXCEEDED;
    status.AddErrorMsg("Mem limit is too low to perform partitioned join. We do not "
        "have enough memory to maintain a buffer per partition.");
    return status;
  }
  return hash_partitions_[partition_idx]->build_rows()->Unpin();
}

Status PartitionedHashJoinNode::ConstructBuildSide(RuntimeState* state) {
  RETURN_IF_ERROR(Expr::Open(build_exprs_, state));
  RETURN_IF_ERROR(Expr::Open(probe_exprs_, state));
  RETURN_IF_ERROR(Expr::Open(other_join_conjuncts_, state));

  // Do a full scan of child(1) and partition the rows.
  RETURN_IF_ERROR(child(1)->Open(state));
  RETURN_IF_ERROR(ProcessBuildInput(state));
  UpdateState(PROCESSING_LEFT_CHILD);
  return Status::OK;
}

Status PartitionedHashJoinNode::ProcessBuildInput(RuntimeState* state) {
  int level = input_partition_ == NULL ? 0 : input_partition_->level_;
  DCHECK(hash_partitions_.empty());
  for (int i = 0; i < PARTITION_FANOUT; ++i) {
    hash_partitions_.push_back(pool_->Add(new Partition(state, this, level + 1)));
    RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->Init());
  }
  LOG(ERROR) << "Initial partitions\n" << DebugString();

  RowBatch build_batch(child(1)->row_desc(), state->batch_size(), mem_tracker());
  bool eos = false;
  while (!eos) {
    RETURN_IF_ERROR(state->CheckQueryState());
    if (input_partition_ == NULL) {
      RETURN_IF_ERROR(child(1)->GetNext(state, &build_batch, &eos));
      COUNTER_UPDATE(build_row_counter_, build_batch.num_rows());
    } else {
      RETURN_IF_ERROR(input_partition_->build_rows()->GetNext(&build_batch, &eos));
    }
    SCOPED_TIMER(build_timer_);
    RETURN_IF_ERROR(ProcessBuildBatch(&build_batch, level));
    build_batch.Reset();
    DCHECK(!build_batch.AtCapacity());
  }
  RETURN_IF_ERROR(BuildHashTables(state));
  return Status::OK;
}

inline bool PartitionedHashJoinNode::AppendRow(BufferedTupleStream* stream, TupleRow* row) {
  if (LIKELY(stream->AddRow(row))) return true;
  status_ = stream->status();
  if (!status_.ok()) return false;
  // We ran out of memory. Pick a partition to spill.
  status_ = SpillPartitions();
  if (!status_.ok()) return false;
  if (!stream->AddRow(row)) {
    // Can this happen? we just spilled a partition so this shouldn't fail.
    status_ = Status("Could not spill row.");
    return false;
  }
  return true;
}

Status PartitionedHashJoinNode::ProcessBuildBatch(RowBatch* build_batch, int level) {
  for (int i = 0; i < build_batch->num_rows(); ++i) {
    TupleRow* row = build_batch->GetRow(i);
    uint32_t hash;
    // TODO: plumb level through when we change the hashing interface. We need different
    // hash functions.
    if (!hash_tbl_->EvalAndHashBuild(row, &hash)) continue;
    Partition* partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];
    bool result = AppendRow(partition->build_rows(), row);
    if (UNLIKELY(!result)) RETURN_IF_ERROR(status_);
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::InitGetNext(TupleRow* first_probe_row) {
  if (first_probe_row == NULL) return Status::OK;
  uint32_t hash;
  if (!hash_tbl_->EvalAndHashProbe(first_probe_row, &hash)) return Status::OK;
  Partition* partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];
  if (partition->hash_tbl() == NULL) {
    // This partition is not in memory, spill the probe row.
    AppendRow(partition->probe_rows(), first_probe_row);
  } else {
    hash_tbl_iterator_= partition->hash_tbl()->Find(first_probe_row);
  }
  return status_;
}

Status PartitionedHashJoinNode::NextLeftChildRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK_EQ(left_batch_pos_, left_batch_->num_rows());
  while (true) {
    // Loop until we find a non-empty row batch.
    if (UNLIKELY(left_batch_pos_ == left_batch_->num_rows())) {
      left_batch_->TransferResourceOwnership(out_batch);
      left_batch_pos_ = 0;
      if (left_side_eos_) break;
      RETURN_IF_ERROR(child(0)->GetNext(state, left_batch_.get(), &left_side_eos_));
      continue;
    }
    return Status::OK;
  }
  current_left_row_ = NULL;
  left_batch_pos_ = -1;
  return Status::OK;
}

Status PartitionedHashJoinNode::NextSpilledRowBatch(
    RuntimeState* state, RowBatch* out_batch) {
  DCHECK(input_partition_ != NULL);
  BufferedTupleStream* probe_rows = input_partition_->probe_rows();
  if (LIKELY(probe_rows->rows_returned() < probe_rows->num_rows())) {
    // Common case, continue from the current probe stream.
    left_batch_->TransferResourceOwnership(out_batch);
    left_batch_pos_ = 0;
    bool eos = false;
    RETURN_IF_ERROR(input_partition_->probe_rows()->GetNext(left_batch_.get(), &eos));
    DCHECK_GT(left_batch_->num_rows(), 0);
  } else {
    // Done with this partition, close it.
    input_partition_->Close();
    input_partition_ = NULL;
    current_left_row_ = NULL;
    left_batch_pos_ = -1;
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::PrepareNextPartition(RuntimeState* state) {
  DCHECK(input_partition_ == NULL);
  if (spilled_partitions_.empty()) return Status::OK;

  int64_t mem_limit = join_node_mem_tracker()->SpareCapacity();
  mem_limit -= state->block_mgr()->block_size();

  input_partition_ = spilled_partitions_.front();
  spilled_partitions_.pop_front();

  DCHECK(input_partition_->hash_tbl() == NULL);
  bool built = false;
  if (input_partition_->EstimatedInMemSize() < mem_limit) {
    RETURN_IF_ERROR(input_partition_->BuildHashTable(state, &built));
  }

  if (!built) {
    if (input_partition_->level_ == MAX_PARTITION_DEPTH) {
      return Status("Build rows have too much skew. Cannot perform join.");
    }
    // This build partition still does not fit in memory. Recurse the algorithm.
    RETURN_IF_ERROR(ProcessBuildInput(state));
    UpdateState(REPARTITIONING);
  } else {
    UpdateState(PROBING_SPILLED_PARTITION);
  }

  RETURN_IF_ERROR(input_partition_->probe_rows()->PrepareForRead());
  return Status::OK;
}

Status PartitionedHashJoinNode::ProcessProbeBatch(RowBatch* out_batch) {
  Expr* const* other_conjuncts = &other_join_conjuncts_[0];
  int num_other_conjuncts = other_join_conjuncts_.size();

  int num_rows_added = 0;
  while (left_batch_pos_ >= 0) {
    while (!hash_tbl_iterator_.AtEnd()) {
      DCHECK(current_left_row_ != NULL);
      TupleRow* matched_build_row = hash_tbl_iterator_.GetRow();
      hash_tbl_iterator_.Next<true>();
      int idx = out_batch->AddRow();
      TupleRow* out_row = out_batch->GetRow(idx);
      CreateOutputRow(out_row, current_left_row_, matched_build_row);

      if (!ExecNode::EvalConjuncts(other_conjuncts, num_other_conjuncts, out_row)) {
        continue;
      }
      out_batch->CommitLastRow();
      ++num_rows_added;
      if (out_batch->AtCapacity()) goto end;
    }

    if (UNLIKELY(left_batch_pos_ == left_batch_->num_rows())) {
      // Finished this batch.
      current_left_row_ = NULL;
      goto end;
    }

    // Establish current_left_row_.
    current_left_row_ = left_batch_->GetRow(left_batch_pos_++);
    Partition* partition = NULL;
    if (input_partition_ != NULL) {
      // We are probing a row from a spilled partition, no need to hash to find
      // the partition.
      partition = input_partition_;
    } else {
      // We don't know which partition this probe row should go to.
      uint32_t hash;
      if (!hash_tbl_->EvalAndHashProbe(current_left_row_, &hash)) continue;
      partition = hash_partitions_[hash >> (32 - NUM_PARTITIONING_BITS)];
    }
    DCHECK(partition != NULL);

    if (partition->hash_tbl() == NULL) {
      if (partition->is_closed()) {
        // This partition is closed, meaning the build side for this partition was empty.
        DCHECK_EQ(state_, PROCESSING_LEFT_CHILD);
        continue;
      }
      // This partition is not in memory, spill the probe row.
      if (UNLIKELY(!AppendRow(partition->probe_rows(), current_left_row_))) {
        return status_;
      }
    } else {
      hash_tbl_iterator_= partition->hash_tbl()->Find(current_left_row_);
    }
  }

end:
  num_rows_returned_ += num_rows_added;
  return Status::OK;
}

Status PartitionedHashJoinNode::GetNext(RuntimeState* state, RowBatch* out_batch,
    bool* eos) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));

  while (true) {
    DCHECK_NE(state_, PARTITIONING_BUILD) << "Should not be in GetNext()";
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(state->CheckQueryState());

    // Finish up the current batch.
    RETURN_IF_ERROR(ProcessProbeBatch(out_batch));
    if (out_batch->AtCapacity() || ReachedLimit()) break;
    DCHECK(current_left_row_ == NULL);

    // Try to continue from the current probe side input.
    if (input_partition_ == NULL) {
      RETURN_IF_ERROR(NextLeftChildRowBatch(state, out_batch));
    } else {
      RETURN_IF_ERROR(NextSpilledRowBatch(state, out_batch));
    }

    // Got a batch, just keep going.
    if (left_batch_pos_ == 0) continue;
    DCHECK_EQ(left_batch_pos_, -1);
    // Finished up all probe rows for hash_partitions_.
    RETURN_IF_ERROR(CleanUpHashPartitions());

    // Move onto the next partition.
    RETURN_IF_ERROR(PrepareNextPartition(state));
    if (input_partition_ == NULL) {
      *eos = true;
      break;
    }
  }

  if (ReachedLimit()) *eos = true;
  return Status::OK;
}

Status PartitionedHashJoinNode::BuildHashTables(RuntimeState* state) {
  int num_remaining_partitions = 0;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->build_rows()->num_rows() == 0) {
      hash_partitions_[i]->Close();
      continue;
    }
    // TODO: this unpin is unnecessary but makes it simple. We should pick the partitions
    // to keep in memory based on how much of the build tuples in that partition are on
    // disk/in memory.
    RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->Unpin());
    ++num_remaining_partitions;
  }
  if (num_remaining_partitions == 0) {
    eos_ = true;
    return Status::OK;
  }

  int64_t max_mem_build_tables = join_node_mem_tracker()->SpareCapacity();
  int num_tables_built = 0;

  if (max_mem_build_tables == -1) max_mem_build_tables = numeric_limits<int64_t>::max();
  // Reserve memory for the buffer needed to handle spilled probe rows.
  int max_num_spilled_partitions = hash_partitions_.size();
  max_mem_build_tables -=
      max_num_spilled_partitions * state->io_mgr()->max_read_buffer_size();

  // Greedily pick the first N partitions until we run out of memory.
  // TODO: We could do better. We know exactly how many rows are in the partition now so
  // this is an optimization problem (i.e. 0-1 knapsack) to find the subset of partitions
  // that fit and result in the least amount of IO.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->EstimatedInMemSize() < max_mem_build_tables) {
      bool built = false;
      RETURN_IF_ERROR(hash_partitions_[i]->BuildHashTable(state, &built));
      if (!built) {
        // Estimate was wrong, cleanup hash table.
        RETURN_IF_ERROR(hash_partitions_[i]->build_rows()->Unpin());
        hash_partitions_[i]->hash_tbl_->Close();
        hash_partitions_[i]->hash_tbl_.reset();
        continue;
      }
      max_mem_build_tables -= hash_partitions_[i]->InMemSize();
      if (++num_tables_built == MAX_IN_MEM_BUILD_TABLES) break;
    }
  }

  // Initialize (reserve one buffer) for each probe partition that needs to spill
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->hash_tbl() != NULL) continue;
    RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->Init());
  }
  return Status::OK;
}

Status PartitionedHashJoinNode::CleanUpHashPartitions() {
  DCHECK_EQ(left_batch_pos_, -1);
  // At this point all the rows have been read from the the probe side for all
  // partitions in hash_partitions_.
  LOG(ERROR) << "Probe Side Consumed\n" << DebugString();
  // Walk the partitions that had hash tables built for the probe phase and close them.
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    if (hash_partitions_[i]->is_closed()) continue;
    if (hash_partitions_[i]->hash_tbl() == NULL) {
      // Unpin the probe stream to free up more memory.
      RETURN_IF_ERROR(hash_partitions_[i]->probe_rows()->Unpin());
      spilled_partitions_.push_back(hash_partitions_[i]);
    } else {
      DCHECK_EQ(hash_partitions_[i]->probe_rows()->num_rows(), 0)
        << "No probe rows should have been spilled for this partition.";
      hash_partitions_[i]->Close();
    }
  }
  hash_partitions_.clear();
  input_partition_ = NULL;
  return Status::OK;
}

void PartitionedHashJoinNode::AddToDebugString(int indent, stringstream* out) const {
  *out << " hash_tbl=";
  *out << string(indent * 2, ' ');
  *out << "HashTbl("
       << " build_exprs=" << Expr::DebugString(build_exprs_)
       << " probe_exprs=" << Expr::DebugString(probe_exprs_);
  *out << ")";
}

void PartitionedHashJoinNode::UpdateState(State s) {
  state_ = s;
  LOG(ERROR) << "Transitioned State:" << endl << DebugString();
}

string PartitionedHashJoinNode::PrintState() const {
  switch (state_) {
    case PARTITIONING_BUILD: return "PartitioningBuild";
    case PROCESSING_LEFT_CHILD: return "ProcessingLeftChild";
    case PROBING_SPILLED_PARTITION: return "ProbingSpilledPartitions";
    case REPARTITIONING: return "Repartioning";
    default: DCHECK(false);
  }
  return "";
}

string PartitionedHashJoinNode::DebugString() const {
  stringstream ss;
  ss << "PartitionedHashJoinNode (state=" << PrintState()
     << " #partitions=" << hash_partitions_.size()
     << " #spilled_partitions=" << spilled_partitions_.size()
     << ")" << endl;
  for (int i = 0; i < hash_partitions_.size(); ++i) {
    ss << i << ": ptr=" << hash_partitions_[i];
    if (hash_partitions_[i]->is_closed()) {
      ss << " closed" << endl;
      continue;
    }
    ss << endl
       << "   Spilled Build Rows: "
       << hash_partitions_[i]->build_rows()->num_rows() << endl;
    if (hash_partitions_[i]->hash_tbl() != NULL) {
      ss << "   Hash Table Rows: " << hash_partitions_[i]->hash_tbl()->size() << endl;
    }
    ss << "   Spilled Probe Rows: "
       << hash_partitions_[i]->probe_rows()->num_rows() << endl;
  }
  return ss.str();
}

