// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#ifndef IMPALA_RUNTIME_SORTER_H_
#define IMPALA_RUNTIME_SORTER_H_

#include <deque>

#include <boost/scoped_ptr.hpp>

#include "runtime/bufferpool/buffer-pool.h"
#include "runtime/exec-env.h"
#include "runtime/runtime-state.h"
#include "util/tuple-row-compare.h"

namespace impala {

class SortedRunMerger;
class RuntimeProfile;
class RowBatch;

/// Sorter contains the external sort implementation. Its purpose is to sort arbitrarily
/// large input data sets with a fixed memory budget by spilling data to disk if
/// necessary.
//
/// The client API for Sorter is as follows:
/// AddBatch() is used to add input rows to be sorted. Multiple tuples in an input row are
/// materialized into a row with a single tuple (the sort tuple) using the materialization
/// exprs in sort_tuple_exprs_. The sort tuples are sorted according to the sort
/// parameters and output by the sorter. AddBatch() can be called multiple times.
//
/// Callers that don't want to spill can use AddBatchNoSpill() instead, which only adds
/// rows up to the memory limit and then returns the number of rows that were added.
/// For this use case, 'enable_spill' should be set to false so that the sorter can reduce
/// the number of buffers requested from the block mgr since there won't be merges.
//
/// InputDone() is called to indicate the end of input. If multiple sorted runs were
/// created, it triggers intermediate merge steps (if necessary) and creates the final
/// merger that returns results via GetNext().
//
/// GetNext() is used to retrieve sorted rows. It can be called multiple times.
/// AddBatch()/AddBatchNoSpill(), InputDone() and GetNext() must be called in that order.
//
/// Batches of input rows are collected into a sequence of pinned BufferPool pages
/// called a run. The maximum size of a run is determined by the number of pages that
/// can be pinned by the Sorter. After the run is full, it is sorted in memory, unpinned
/// and the next run is constructed. The variable-length column data (e.g. string slots)
/// in the materialized sort tuples are stored in a separate sequence of pages from the
/// tuples themselves.  When the pages containing tuples in a run are unpinned, the
/// var-len slot pointers are converted to offsets from the start of the first var-len
/// data page. When a page is read back, these offsets are converted back to pointers.
/// The in-memory sorter sorts the fixed-length tuples in-place. The output rows have the
/// same schema as the materialized sort tuples.
//
/// After the input is consumed, the sorter is left with one or more sorted runs. If
/// there are multiple runs, the runs are merged using SortedRunMerger. At least one
/// page per run (two if there are var-length slots) must be pinned in memory during
/// a merge, so multiple merges may be necessary if the number of runs is too large.
/// First a series of intermediate merges are performed, until the number of runs is
/// small enough to do a single final merge that returns batches of sorted rows to the
/// caller of GetNext().
///
/// If there is a single sorted run (i.e. no merge required), only tuple rows are
/// copied into the output batch supplied by GetNext(), and the data itself is left in
/// pinned pages held by the sorter.
///
/// When merges are performed, one input batch is created to hold tuple rows for each
/// input run, and one batch is created to hold deep copied rows (i.e. ptrs + data) from
/// the output of the merge.
//
/// Note that Init() must be called right after the constructor.
//
/// During a merge, one row batch is created for each input run, and one batch is created
/// for the output of the merge (if is not the final merge). It is assumed that the memory
/// for these batches have already been accounted for in the memory budget for the sort.
/// That is, the memory for these batches does not come out of the buffer pool.
//
/// TODO: Not necessary to actually copy var-len data - instead take ownership of the
/// var-length data in the input batch. Copying can be deferred until a run is unpinned.
/// TODO: When the first run is constructed, create a sequence of pointers to materialized
/// tuples. If the input fits in memory, the pointers can be sorted instead of sorting the
/// tuples in place.
class Sorter {
 public:
  /// 'sort_tuple_exprs' are the slot exprs used to materialize the tuples to be
  /// sorted. 'ordering_exprs', 'is_asc_order' and 'nulls_first' are parameters
  /// for the comparator for the sort tuples.
  /// 'node_id' is the ID of the exec node using the sorter for error reporting.
  /// 'enable_spilling' should be set to false to reduce the number of requested buffers
  /// if the caller will use AddBatchNoSpill().
  ///
  /// The Sorter assumes that it has exclusive use of the client's
  /// reservations for sorting, and may increase the size of the client's reservation.
  /// The caller is responsible for ensuring that the minimum reservation (returned from
  /// ComputeMinReservation()) is available.
  Sorter(const std::vector<ScalarExpr*>& ordering_exprs,
      const std::vector<bool>& is_asc_order, const std::vector<bool>& nulls_first,
      const std::vector<ScalarExpr*>& sort_tuple_exprs, RowDescriptor* output_row_desc,
      MemTracker* mem_tracker, BufferPool::ClientHandle* client, int64_t page_len,
      RuntimeProfile* profile, RuntimeState* state, int node_id,
      bool enable_spilling);
  ~Sorter();

  /// Initial set-up of the sorter for execution.
  /// The evaluators for 'sort_tuple_exprs_' will be created and stored in 'obj_pool'.
  Status Prepare(ObjectPool* obj_pool) WARN_UNUSED_RESULT;

  /// Do codegen for the Sorter. Called after Prepare() if codegen is desired. Returns OK
  /// if successful or a Status describing the reason why Codegen failed otherwise.
  Status Codegen(RuntimeState* state);

  /// Opens the sorter for adding rows and initializes the evaluators for materializing
  /// the tuples. Must be called after Prepare() or Reset() and before calling AddBatch().
  Status Open() WARN_UNUSED_RESULT;

  /// Adds the entire batch of input rows to the sorter. If the current unsorted run fills
  /// up, it is sorted and a new unsorted run is created. Cannot be called if
  /// 'enable_spill' is false.
  Status AddBatch(RowBatch* batch) WARN_UNUSED_RESULT;

  /// Adds input rows to the current unsorted run, starting from 'start_index' up to the
  /// memory limit. Returns the number of rows added in 'num_processed'.
  Status AddBatchNoSpill(
      RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT;

  /// Called to indicate there is no more input. Triggers the creation of merger(s) if
  /// necessary.
  Status InputDone() WARN_UNUSED_RESULT;

  /// Get the next batch of sorted output rows from the sorter.
  Status GetNext(RowBatch* batch, bool* eos) WARN_UNUSED_RESULT;

  /// Resets all internal state like ExecNode::Reset().
  /// Init() must have been called, AddBatch()/GetNext()/InputDone()
  /// may or may not have been called.
  void Reset();

  /// Close the Sorter and free resources.
  void Close(RuntimeState* state);

  /// Compute the minimum amount of buffer memory in bytes required to execute a
  /// sort with the current sorter.
  int64_t ComputeMinReservation();

  /// Return true if the sorter has any spilled runs.
  bool HasSpilledRuns() const;

 private:

  /// Wrapper around BufferPool::PageHandle that tracks additional info about the page.
  /// The Page can be in four states:
  /// * Closed: The page starts in this state before Init() is called. Calling
  ///   ExtractBuffer() or Close() puts the page back in this state. No other operations
  ///   are valid on a closed page.
  /// * In memory: the page is pinned and the buffer is in memory. data() is valid. The
  ///   page is in this state after Init(). If the page is pinned but not in memory, it
  ///   can be brought into this state by calling WaitForBuffer().
  /// * Unpinned: the page was unpinned by calling Unpin(). It is invalid to access the
  ///   page's buffer.
  /// * Pinned but not in memory: Pin() was called on the unpinned page, but
  ///   WaitForBuffer() has not been called. It is invalid to access the page's buffer.
  class Page {
   public:
    Page() { Reset(); }

    /// Create a new page of length 'sorter->page_len_' bytes using
    /// 'sorter->buffer_pool_client_'. Caller must ensure the client has enough
    /// reservation for the page.
    Status Init(Sorter* sorter) WARN_UNUSED_RESULT;

    /// Extract the buffer from the page. The page must be in memory. When this function
    /// returns the page is closed.
    BufferPool::BufferHandle ExtractBuffer(BufferPool::ClientHandle* client);

    /// Allocate 'len' bytes in the current page. The page must be in memory, and the
    /// amount to allocate cannot exceed BytesRemaining().
    uint8_t* AllocateBytes(int64_t len);

    /// Free the last 'len' bytes allocated from AllocateBytes(). The page must be in
    /// memory.
    void FreeBytes(int64_t len);

    /// Return number of bytes remaining in page.
    int64_t BytesRemaining() { return len() - valid_data_len_; }

    /// Brings a pinned page into memory, if not already in memory, and sets 'data_' to
    /// point to the page's buffer.
    Status WaitForBuffer() WARN_UNUSED_RESULT;

    /// Helper to pin the page. Caller must ensure the client has enough reservation
    /// remaining to pin the page. Only valid to call on an unpinned page.
    Status Pin(BufferPool::ClientHandle* client) WARN_UNUSED_RESULT;

    /// Helper to unpin the page.
    void Unpin(BufferPool::ClientHandle* client);

    /// Destroy the page with 'client'.
    void Close(BufferPool::ClientHandle* client);

    int64_t valid_data_len() const { return valid_data_len_; }
    /// Returns a pointer to the start of the page's buffer. Only valid to call if the
    /// page is in memory.
    uint8_t* data() const {
      DCHECK(data_ != nullptr);
      return data_;
    }
    int64_t len() const { return handle_.len(); }
    bool is_open() const { return handle_.is_open(); }
    bool is_pinned() const { return handle_.is_pinned(); }
    std::string DebugString() const { return handle_.DebugString(); }

   private:
    /// Reset the page to an unitialized state. 'handle_' must already be closed.
    void Reset();

    /// Helper to get the singleton buffer pool.
    static BufferPool* pool() { return ExecEnv::GetInstance()->buffer_pool(); }

    BufferPool::PageHandle handle_;

    /// Length of valid data written to the page.
    int64_t valid_data_len_;

    /// Cached pointer to the buffer in 'handle_'. NULL if the page is unpinned. May be NULL
    /// or not NULL if the page is pinned. Can be populated by calling WaitForBuffer() on a
    /// pinned page.
    uint8_t* data_;
  };

  class TupleIterator;

  /// A run is a sequence of tuples. The run can be sorted or unsorted (in which case the
  /// Sorter will sort it). A run comprises a sequence of fixed-length pages containing the
  /// tuples themselves (i.e. fixed-len slots that may contain ptrs to var-length data), and
  /// an optional sequence of var-length pages containing the var-length data.
  ///
  /// Runs are either "initial runs" constructed from the sorter's input by evaluating
  /// the expressions in 'sort_tuple_exprs_' or "intermediate runs" constructed
  /// by merging already-sorted runs. Initial runs are sorted in-place in memory. Once
  /// sorted, runs can be spilled to disk to free up memory. Sorted runs are merged by
  /// SortedRunMerger, either to produce the final sorted output or to produce another
  /// sorted run.
  ///
  /// The expected calling sequence of functions is as follows:
  /// * Init() to initialize the run and allocate initial pages.
  /// * Add*Batch() to add batches of tuples to the run.
  /// * FinalizeInput() to signal that no more batches will be added.
  /// * If the run is unsorted, it must be sorted. After that set_sorted() must be called.
  /// * Once sorted, the run is ready to read in sorted order for merging or final output.
  /// * PrepareRead() to allocate resources for reading the run.
  /// * GetNext() (if there was a single run) or GetNextBatch() (when merging multiple runs)
  ///   to read from the run.
  /// * Once reading is done, CloseAllPages() should be called to free resources.
  class Run {
   public:
    Run(Sorter* parent, TupleDescriptor* sort_tuple_desc, bool initial_run);

    ~Run();

    /// Initialize the run for input rows by allocating the minimum number of required
    /// pages - one page for fixed-len data added to fixed_len_pages_, one for the
    /// initially unsorted var-len data added to var_len_pages_, and one to copy sorted
    /// var-len data into var_len_copy_page_.
    Status Init() WARN_UNUSED_RESULT;

    /// Add the rows from 'batch' starting at 'start_index' to the current run. Returns the
    /// number of rows actually added in 'num_processed'. If the run is full (no more pages
    /// can be allocated), 'num_processed' may be less than the number of remaining rows in
    /// the batch. AddInputBatch() materializes the input rows using the expressions in
    /// sorter_->sort_tuple_expr_evals_, while AddIntermediateBatch() just copies rows.
    Status AddInputBatch(
        RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT;

    Status AddIntermediateBatch(
        RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT;

    /// Called after the final call to Add*Batch() to do any bookkeeping necessary to
    /// finalize the run. Must be called before sorting or merging the run.
    Status FinalizeInput() WARN_UNUSED_RESULT;

    /// Unpins all the pages in a sorted run. Var-length column data is copied into new
    /// pages in sorted order. Pointers in the original tuples are converted to offsets
    /// from the beginning of the sequence of var-len data pages. Returns an error and
    /// may leave some pages pinned if an error is encountered.
    Status UnpinAllPages() WARN_UNUSED_RESULT;

    /// Closes all pages and clears vectors of pages.
    void CloseAllPages();

    /// Prepare to read a sorted run. Pins the first page(s) in the run if the run was
    /// previously unpinned. If the run was unpinned, try to pin the initial fixed and
    /// var len pages in the run. If it couldn't pin them, set pinned to false.
    /// In that case, none of the initial pages will be pinned and it is valid to
    /// call PrepareRead() again to retry pinning. pinned is always set to
    /// true if the run was pinned.
    Status PrepareRead(bool* pinned) WARN_UNUSED_RESULT;

    /// Interface for merger - get the next batch of rows from this run. This run still
    /// owns the returned batch. Calls GetNext(RowBatch*, bool*).
    Status GetNextBatch(RowBatch** sorted_batch) WARN_UNUSED_RESULT;

    /// Fill output_batch with rows from this run. If CONVERT_OFFSET_TO_PTR is true, offsets
    /// in var-length slots are converted back to pointers. Only row pointers are copied
    /// into output_batch. eos is set to true after all rows from the run are returned.
    /// If eos is true, the returned output_batch has zero rows and has no attached pages.
    /// If this run was unpinned, one page (two if there are var-len slots) is pinned while
    /// rows are filled into output_batch. The page is unpinned before the next page is
    /// pinned, so at most one (two if there are var-len slots) page(s) will be pinned at
    /// once. If the run was pinned, the pages are not unpinned and each page is attached
    /// to 'output_batch' once all rows referencing data in the page have been returned,
    /// either in the current batch or previous batches. In both pinned and unpinned cases,
    /// all rows in output_batch will reference at most one fixed-len and one var-len page.
    template <bool CONVERT_OFFSET_TO_PTR>
    Status GetNext(RowBatch* output_batch, bool* eos) WARN_UNUSED_RESULT;

    /// Delete all pages in 'runs' and clear 'runs'.
    static void CleanupRuns(std::deque<Run*>* runs);

    /// Return total amount of fixed and var len data in run, not including pages that
    /// were already transferred or closed.
    int64_t TotalBytes() const;

    bool is_pinned() const { return is_pinned_; }
    bool is_finalized() const { return is_finalized_; }
    bool is_sorted() const { return is_sorted_; }
    void set_sorted() { is_sorted_ = true; }
    int64_t num_tuples() const { return num_tuples_; }

   private:
    /// TupleIterator needs access to internals to iterate over tuples.
    friend class TupleIterator;

    /// Templatized implementation of Add*Batch() functions.
    /// INITIAL_RUN and HAS_VAR_LEN_SLOTS are template arguments for performance and must
    /// match 'initial_run_' and 'has_var_len_slots_'.
    template <bool HAS_VAR_LEN_SLOTS, bool INITIAL_RUN>
    Status AddBatchInternal(
        RowBatch* batch, int start_index, int* num_processed) WARN_UNUSED_RESULT;

    /// Finalize the list of pages: delete empty final pages and unpin the previous page
    /// if the run is unpinned.
    Status FinalizePages(vector<Page>* pages) WARN_UNUSED_RESULT;

    /// Collect the non-null var-len (e.g. STRING) slots from 'src' in 'var_len_values' and
    /// return the total length of all var-len values in 'total_var_len'.
    void CollectNonNullVarSlots(
        Tuple* src, vector<StringValue*>* var_len_values, int* total_var_len);

    enum AddPageMode { KEEP_PREV_PINNED, UNPIN_PREV };

    /// Try to extend the current run by a page. If 'mode' is KEEP_PREV_PINNED, try to
    /// allocate a new page, which may fail to extend the run due to lack of memory. If
    /// mode is 'UNPIN_PREV', unpin the previous page in page_sequence before allocating
    /// and adding a new page - this never fails due to lack of memory.
    ///
    /// Returns an error status only if the buffer pool returns an error. If no error is
    /// encountered, sets 'added' to indicate whether the run was extended and returns
    /// Status::OK(). The new page is appended to 'page_sequence'.
    Status TryAddPage(
        AddPageMode mode, vector<Page>* page_sequence, bool* added) WARN_UNUSED_RESULT;

    /// Adds a new page to 'page_sequence' by a page. Caller must ensure enough
    /// reservation is available to create the page.
    ///
    /// Returns an error status only if the buffer pool returns an error. If an error
    /// is returned 'page_sequence' is left unmodified.
    Status AddPage(vector<Page>* page_sequence) WARN_UNUSED_RESULT;

    /// Advance to the next read page. If the run is pinned, has no effect. If the run
    /// is unpinned, atomically pin the page at 'page_index' + 1 in 'pages' and delete
    /// the page at 'page_index'.
    Status PinNextReadPage(vector<Page>* pages, int page_index) WARN_UNUSED_RESULT;

    /// Copy the StringValues in 'var_values' to 'dest' in order and update the StringValue
    /// ptrs in 'dest' to point to the copied data.
    void CopyVarLenData(const vector<StringValue*>& var_values, uint8_t* dest);

    /// Copy the StringValues in 'var_values' to 'dest' in order. Update the StringValue
    /// ptrs in 'dest' to contain a packed offset for the copied data comprising
    /// page_index and the offset relative to page_start.
    void CopyVarLenDataConvertOffset(const vector<StringValue*>& var_values, int page_index,
        const uint8_t* page_start, uint8_t* dest);

    /// Convert encoded offsets to valid pointers in tuple with layout 'sort_tuple_desc_'.
    /// 'tuple' is modified in-place. Returns true if the pointers refer to the page at
    /// 'var_len_pages_index_' and were successfully converted or false if the var len
    /// data is in the next page, in which case 'tuple' is unmodified.
    bool ConvertOffsetsToPtrs(Tuple* tuple);

    /// Returns true if we have var-len pages in the run.
    bool HasVarLenPages() const;

    static int NumOpenPages(const vector<Page>& pages);

    /// Close all open pages and clear vector.
    void DeleteAndClearPages(vector<Page>* pages);

    /// Parent sorter object.
    Sorter* const sorter_;

    /// Materialized sort tuple. Input rows are materialized into 1 tuple (with descriptor
    /// sort_tuple_desc_) before sorting.
    const TupleDescriptor* sort_tuple_desc_;

    /// The size in bytes of the sort tuple.
    const int sort_tuple_size_;

    /// Number of tuples per page in a run. This gets multiplied with
    /// TupleIterator::page_index_ in various places and to make sure we don't overflow the
    /// result of that operation we make this int64_t here.
    const int64_t page_capacity_;

    const bool has_var_len_slots_;

    /// True if this is an initial run. False implies this is an sorted intermediate run
    /// resulting from merging other runs.
    const bool initial_run_;

    /// True if all pages in the run are pinned. Initial runs start off pinned and
    /// can be unpinned. Intermediate runs are always unpinned.
    bool is_pinned_;

    /// True after FinalizeInput() is called. No more tuples can be added after the
    /// run is finalized.
    bool is_finalized_;

    /// True if the tuples in the run are currently in sorted order.
    /// Always true for intermediate runs.
    bool is_sorted_;

    /// Sequence of pages in this run containing the fixed-length portion of the sort
    /// tuples comprising this run. The data pointed to by the var-len slots are in
    /// var_len_pages_. A run can have zero pages if no rows are appended.
    /// If the run is sorted, the tuples in fixed_len_pages_ will be in sorted order.
    /// fixed_len_pages_[i] is closed iff it has been transferred or deleted.
    vector<Page> fixed_len_pages_;

    /// Sequence of pages in this run containing the var-length data corresponding to the
    /// var-length columns from fixed_len_pages_. In intermediate runs, the var-len data is
    /// always stored in the same order as the fixed-length tuples. In initial runs, the
    /// var-len data is initially in unsorted order, but is reshuffled into sorted order in
    /// UnpinAllPages(). A run can have no var len pages if there are no var len slots or
    /// if all the var len data is empty or NULL.
    /// var_len_pages_[i] is closed iff it has been transferred or deleted.
    vector<Page> var_len_pages_;

    /// For initial unsorted runs, an extra pinned page is needed to reorder var-len data
    /// into fixed order in UnpinAllPages(). 'var_len_copy_page_' stores this extra
    /// page. Deleted in UnpinAllPages().
    /// TODO: in case of in-memory runs, this could be deleted earlier to free up memory.
    Page var_len_copy_page_;

    /// Number of tuples added so far to this run.
    int64_t num_tuples_;

    /// Number of tuples returned via GetNext(), maintained for debug purposes.
    int64_t num_tuples_returned_;

    /// Used to implement GetNextBatch() interface required for the merger.
    boost::scoped_ptr<RowBatch> buffered_batch_;

    /// Members used when a run is read in GetNext().
    /// The index into 'fixed_' and 'var_len_pages_' of the pages being read in GetNext().
    int fixed_len_pages_index_;
    int var_len_pages_index_;

    /// If true, the last call to GetNext() reached the end of the previous fixed or
    /// var-len page. The next call to GetNext() must increment 'fixed_len_pages_index_'
    /// or 'var_len_pages_index_'. It must also pin the next page if the run is unpinned.
    bool end_of_fixed_len_page_;
    bool end_of_var_len_page_;

    /// Offset into the current fixed length data page being processed.
    int fixed_len_page_offset_;
  };

  /// Helper class used to iterate over tuples in a run during sorting.
  class TupleIterator {
   public:
    /// Creates an iterator pointing at the tuple with the given 'index' in the 'run'.
    /// The index can be in the range [0, run->num_tuples()]. If it is equal to
    /// run->num_tuples(), the iterator points to one past the end of the run, so
    /// invoking Prev() will cause the iterator to point at the last tuple in the run.
    /// 'run' must be finalized.
    TupleIterator(Sorter::Run* run, int64_t index);

    /// Default constructor used for local variable. Produces invalid iterator that must
    /// be assigned before use.
    TupleIterator() : index_(-1), tuple_(nullptr), buffer_start_index_(-1),
        buffer_end_index_(-1), page_index_(-1) { }

    /// Create an iterator pointing to the first tuple in the run.
    static TupleIterator Begin(Sorter::Run* run) { return {run, 0}; }

    /// Create an iterator pointing one past the end of the run.
    static TupleIterator End(Sorter::Run* run) { return {run, run->num_tuples()}; }

    /// Increments 'index_' and sets 'tuple_' to point to the next tuple in the run.
    /// Increments 'page_index_' and advances to the next page if the next tuple is in
    /// the next page. Can be advanced one past the last tuple in the run, but is not
    /// valid to dereference 'tuple_' in that case. 'run' and 'tuple_size' are passed as
    /// arguments to avoid redundantly storing the same values in multiple iterators in
    /// perf-critical algorithms.
    void Next(Sorter::Run* run, int tuple_size);

    /// The reverse of Next(). Can advance one before the first tuple in the run, but it is
    /// invalid to dereference 'tuple_' in that case.
    void Prev(Sorter::Run* run, int tuple_size);

    int64_t index() const { return index_; }
    Tuple* tuple() const { return reinterpret_cast<Tuple*>(tuple_); }
    /// Returns current tuple in TupleRow format. The caller should not modify the row.
    const TupleRow* row() const {
      return reinterpret_cast<const TupleRow*>(&tuple_);
    }

   private:
    // Move to the next page in the run (or do nothing if at end of run).
    // This is the slow path for Next();
    void NextPage(Sorter::Run* run, int tuple_size);

    // Move to the previous page in the run (or do nothing if at beginning of run).
    // This is the slow path for Prev();
    void PrevPage(Sorter::Run* run, int tuple_size);

    /// Index of the current tuple in the run.
    /// Can be -1 or run->num_rows() if Next() or Prev() moves iterator outside of run.
    int64_t index_;

    /// Pointer to the current tuple.
    /// Will be an invalid pointer outside of current buffer if Next() or Prev() moves
    /// iterator outside of run.
    uint8_t* tuple_;

    /// Indices of start and end tuples of page at page_index_. I.e. the current page
    /// has tuples with indices in range [buffer_start_index_, buffer_end_index).
    int64_t buffer_start_index_;
    int64_t buffer_end_index_;

    /// Index into fixed_len_pages_ of the page containing the current tuple.
    /// If index_ is negative or past end of run, will point to the first or last page
    /// in run respectively.
    int page_index_;
  };

  /// Sorts a sequence of tuples from a run in place using a provided tuple comparator.
  /// Quick sort is used for sequences of tuples larger that 16 elements, and insertion sort
  /// is used for smaller sequences. The TupleSorter is initialized with a RuntimeState
  /// instance to check for cancellation during an in-memory sort.
  class TupleSorter {
   public:
    TupleSorter(Sorter* parent, const TupleRowComparator& comparator, int64_t page_size,
        int tuple_size, RuntimeState* state);

    ~TupleSorter();

    /// Performs a quicksort for tuples in 'run' followed by an insertion sort to
    /// finish smaller ranges. Only valid to call if this is an initial run that has not
    /// yet been sorted. Returns an error status if any error is encountered or if the
    /// query is cancelled.
    Status Sort(Run* run) WARN_UNUSED_RESULT;

    Status Codegen(llvm::Function* ComparatorLessFn, RuntimeState* state);

   private:
    static const int INSERTION_THRESHOLD = 16;

    Sorter* const parent_;

    /// Size of the tuples in memory.
    const int tuple_size_;

    /// Tuple comparator with method Less() that returns true if lhs < rhs.
    const TupleRowComparator& comparator_;

    /// Number of times comparator_.Less() can be invoked again before
    /// comparator_. expr_results_pool_.Clear() needs to be called.
    int num_comparisons_till_free_;

    /// Runtime state instance to check for cancellation. Not owned.
    RuntimeState* const state_;

    /// The run to be sorted.
    Run* run_;

    /// Temporarily allocated space to copy and swap tuples (Both are used in Partition()).
    /// Owned by this TupleSorter instance.
    uint8_t* temp_tuple_buffer_;
    uint8_t* swap_buffer_;

    /// Random number generator used to randomly choose pivots. We need a RNG that
    /// can generate 64-bit ints. Quality of randomness doesn't need to be especially
    /// high: Mersenne Twister should be more than adequate.
    std::mt19937_64 rng_;

    Status (*codegened_sort_helper_) (TupleSorter*, TupleIterator, TupleIterator);

    /// Wrapper around comparator_.Less(). Also call expr_results_pool_.Clear()
    /// on every 'state_->batch_size()' invocations of comparator_.Less(). Returns true
    /// if 'lhs' is less than 'rhs'.
    bool Less(const TupleRow* lhs, const TupleRow* rhs);

    /// Perform an insertion sort for rows in the range [begin, end) in a run.
    /// Only valid to call for ranges of size at least 1.
    Status InsertionSort(
        const TupleIterator& begin, const TupleIterator& end) WARN_UNUSED_RESULT;

    /// Partitions the sequence of tuples in the range [begin, end) in a run into two
    /// groups around the pivot tuple - i.e. tuples in first group are <= the pivot, and
    /// tuples in the second group are >= pivot. Tuples are swapped in place to create the
    /// groups and the index to the first element in the second group is returned in 'cut'.
    /// Return an error status if any error is encountered or if the query is cancelled.
    Status Partition(TupleIterator begin, TupleIterator end,
        const Tuple* pivot, TupleIterator* cut);

    /// Performs a quicksort of rows in the range [begin, end) followed by insertion sort
    /// for smaller groups of elements. Return an error status for any errors or if the
    /// query is cancelled.
    Status SortHelper(TupleIterator begin, TupleIterator end) WARN_UNUSED_RESULT;

    /// Select a pivot to partition [begin, end).
    Tuple* SelectPivot(TupleIterator begin, TupleIterator end);

    /// Return median of three tuples according to the sort comparator.
    Tuple* MedianOfThree(Tuple* t1, Tuple* t2, Tuple* t3);

    /// Swaps tuples pointed to by left and right using 'swap_tuple'.
    static void Swap(Tuple* left, Tuple* right, Tuple* swap_tuple, int tuple_size);
  };


  /// Create a SortedRunMerger from sorted runs in 'sorted_runs_' and assign it to
  /// 'merger_'. Attempts to set up merger with 'max_num_runs' runs but may set it
  /// up with fewer if it cannot pin the initial pages of all of the runs. Fails
  /// if it cannot merge at least two runs. The runs to be merged are removed from
  /// 'sorted_runs_'.  The Sorter sets the 'deep_copy_input' flag to true for the
  /// merger, since the pages containing input run data will be deleted as input
  /// runs are read.
  Status CreateMerger(int max_num_runs) WARN_UNUSED_RESULT;

  /// Repeatedly replaces multiple smaller runs in sorted_runs_ with a single larger
  /// merged run until there are few enough runs to be merged with a single merger.
  /// Returns when 'merger_' is set up to merge the final runs.
  /// At least 1 (2 if var-len slots) page from each sorted run must be pinned for
  /// a merge. If the number of sorted runs is too large, merge sets of smaller runs
  /// into large runs until a final merge can be performed. An intermediate row batch
  /// containing deep copied rows is used for the output of each intermediate merge.
  Status MergeIntermediateRuns() WARN_UNUSED_RESULT;

  /// Execute a single step of the intermediate merge, pulling rows from 'merger_'
  /// and adding them to 'merged_run'.
  Status ExecuteIntermediateMerge(Sorter::Run* merged_run) WARN_UNUSED_RESULT;

  /// Called once there no more rows to be added to 'unsorted_run_'. Sorts
  /// 'unsorted_run_' and appends it to the list of sorted runs.
  Status SortCurrentInputRun() WARN_UNUSED_RESULT;

  /// Helper that cleans up all runs in the sorter.
  void CleanupAllRuns();

  /// ID of the ExecNode that owns the sorter, used for error reporting.
  const int node_id_;

  /// Runtime state instance used to check for cancellation. Not owned.
  RuntimeState* const state_;

  /// MemPool for allocating data structures used by expression evaluators in the sorter.
  MemPool expr_perm_pool_;

  /// MemPool for allocations that hold results of expression evaluation in the sorter.
  /// Cleared periodically during sorting to prevent memory accumulating.
  MemPool expr_results_pool_;

  /// In memory sorter and less-than comparator.
  TupleRowComparator compare_less_than_;
  boost::scoped_ptr<TupleSorter> in_mem_tuple_sorter_;

  /// Client used to allocate pages from the buffer pool. Not owned.
  BufferPool::ClientHandle* const buffer_pool_client_;

  /// The length of page to use.
  const int64_t page_len_;

  /// True if the tuples to be sorted have var-length slots.
  bool has_var_len_slots_;

  /// Expressions used to materialize the sort tuple. One expr per slot in the tuple.
  const std::vector<ScalarExpr*>& sort_tuple_exprs_;
  std::vector<ScalarExprEvaluator*> sort_tuple_expr_evals_;

  /// Mem tracker for batches created during merge. Not owned by Sorter.
  MemTracker* mem_tracker_;

  /// Descriptor for the sort tuple. Input rows are materialized into 1 tuple before
  /// sorting. Not owned by the Sorter.
  RowDescriptor* output_row_desc_;

  /// True if this sorter can spill. Used to determine the number of buffers to reserve.
  bool enable_spilling_;

  /////////////////////////////////////////
  /// BEGIN: Members that must be Reset()

  /// The current unsorted run that is being collected. Is sorted and added to
  /// sorted_runs_ after it is full (i.e. number of pages allocated == max available
  /// buffers) or after the input is complete. Owned and placed in obj_pool_.
  /// When it is added to sorted_runs_, it is set to NULL.
  Run* unsorted_run_;

  /// List of sorted runs that have been produced but not merged. unsorted_run_ is added
  /// to this list after an in-memory sort. Sorted runs produced by intermediate merges
  /// are also added to this list during the merge. Runs are added to the object pool.
  std::deque<Run*> sorted_runs_;

  /// Merger object (intermediate or final) currently used to produce sorted runs.
  /// Only one merge is performed at a time. Will never be used if the input fits in
  /// memory.
  boost::scoped_ptr<SortedRunMerger> merger_;

  /// Spilled runs that are currently processed by the merge_.
  /// These runs can be deleted when we are done with the current merge.
  std::deque<Run*> merging_runs_;

  /// Output run for the merge. Stored in Sorter() so that it can be cleaned up
  /// in Sorter::Close() in case of errors.
  Run* merge_output_run_;

  /// Pool of owned Run objects. Maintains Runs objects across non-freeing Reset() calls.
  ObjectPool run_pool_;

  /// END: Members that must be Reset()
  /////////////////////////////////////////

  /// Runtime profile and counters for this sorter instance.
  RuntimeProfile* profile_;

  /// Pool of objects (e.g. exprs) that are not freed during Reset() calls.
  ObjectPool obj_pool_;

  /// Number of initial runs created.
  RuntimeProfile::Counter* initial_runs_counter_;

  /// Number of runs that were unpinned and may have spilled to disk, including initial
  /// and intermediate runs.
  RuntimeProfile::Counter* spilled_runs_counter_;

  /// Number of merges of sorted runs.
  RuntimeProfile::Counter* num_merges_counter_;

  /// Time spent sorting initial runs in memory.
  RuntimeProfile::Counter* in_mem_sort_timer_;

  /// Total size of the initial runs in bytes.
  RuntimeProfile::Counter* sorted_data_size_;

  /// Min, max, and avg size of runs in number of tuples.
  RuntimeProfile::SummaryStatsCounter* run_sizes_;
};

} // namespace impala

#endif
