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


#ifndef IMPALA_UTIL_TUPLE_ROW_COMPARE_H_
#define IMPALA_UTIL_TUPLE_ROW_COMPARE_H_

#include "common/compiler-util.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/descriptors.h"
#include "runtime/raw-value.h"
#include "runtime/raw-value.inline.h"
#include "runtime/tuple.h"
#include "runtime/tuple-row.h"

namespace impala {

class RuntimeState;
class ScalarExprEvaluator;

/// A wrapper around types Comparator with a Less() method. This wrapper allows the use of
/// type Comparator with STL containers which expect a type like std::less<T>, which uses
/// operator() instead of Less() and is cheap to copy.
///
/// The C++ standard requires that std::priority_queue operations behave as wrappers to
/// {push,pop,make,sort}_heap, which take their comparator object by value. Therefore, it
/// is inefficient to use comparator objects that have expensive construction,
/// destruction, and copying with std::priority_queue.
///
/// ComparatorWrapper takes a reference to an object of type Comparator, rather than
/// copying that object. ComparatorWrapper<Comparator>(comp) is not safe to use beyond the
/// lifetime of comp.
template <typename Comparator>
class ComparatorWrapper {
  const Comparator& comp_;
 public:
  ComparatorWrapper(const Comparator& comp) : comp_(comp) {}

  template <typename T>
  bool operator()(const T& lhs, const T& rhs) const {
    return comp_.Less(lhs, rhs);
  }
};

/// Compares two TupleRows based on a set of exprs, in order.
struct TupleRowComparator {
  /// 'ordering_exprs': the ordering expressions for tuple comparison.
  /// 'is_asc' determines, for each expr, if it should be ascending or descending sort
  /// order.
  /// 'nulls_first' determines, for each expr, if nulls should come before or after all
  /// other values.
  TupleRowComparator(const std::vector<ScalarExpr*>& ordering_exprs,
      const std::vector<bool>& is_asc, const std::vector<bool>& nulls_first)
    : ordering_exprs_(ordering_exprs), is_asc_(is_asc), codegend_compare_fn_(nullptr) {
    DCHECK_EQ(is_asc_.size(), ordering_exprs.size());
    for (bool null_first : nulls_first) nulls_first_.push_back(null_first ? -1 : 1);
  }

  /// Create the evaluators for the ordering expressions and store them in 'pool'. The
  /// evaluators use 'expr_perm_pool' and 'expr_results_pool' for permanent and result
  /// allocations made by exprs respectively.
  Status Prepare(ObjectPool* pool, RuntimeState* state, MemPool* expr_perm_pool,
      MemPool* expr_results_pool);

  /// Initialize the evaluators using 'state'.
  Status Open(RuntimeState* state);

  /// Release resources held by the ordering expressions' evaluators.
  void Close(RuntimeState* state);

  /// Returns true if lhs is strictly less than rhs.
  /// ordering_exprs must have been prepared and opened before calling this.
  /// Force inlining because it tends not to be always inlined at callsites, even in
  /// hot loops.
  bool ALWAYS_INLINE Less(const TupleRow* lhs, const TupleRow* rhs) const {
    return CompareInterpreted(lhs, rhs) < 0;
  }

  bool ALWAYS_INLINE Less(const Tuple* lhs, const Tuple* rhs) const {
    return Less(reinterpret_cast<TupleRow*>(&lhs), reinterpret_cast<TupleRow*>(&rhs));
  }

  /// Use codegened version if available.
  bool ALWAYS_INLINE MaybeCodegenedLess(const TupleRow* lhs, const TupleRow* rhs) const{
      return codegend_compare_fn_ == nullptr ? Less(lhs, rhs) :
          codegend_compare_fn_(this, lhs, rhs) < 0;
  }

  /// Codegen Compare() and compile it into a function pointer. The resulting function
  /// pointer can be used by MaybeCodegenedLess() in circumstances where inlining
  /// Compare() hasn't been implemented.
  Status Codegen(RuntimeState* state);

  /// Codegen Compare() and write the resulting llvm function into 'fn'. The caller
  /// could use it to replaced cross-compiled CompareInterpreted().
  Status CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn);

  /// Struct name in LLVM IR.
  static const char* LLVM_CLASS_NAME;

  /// Interpreted implementation of Compare().
  int IR_NO_INLINE CompareInterpreted(const TupleRow* lhs, const TupleRow* rhs) const;

  /// References to ordering expressions owned by the Exec node which owns this
  /// TupleRowComparator.
  const std::vector<ScalarExpr*>& ordering_exprs_;

  /// The evaluators for the LHS and RHS ordering expressions. The RHS evaluator is
  /// created via cloning the evaluator after it has been Opened().
  std::vector<ScalarExprEvaluator*> ordering_expr_evals_lhs_;
  std::vector<ScalarExprEvaluator*> ordering_expr_evals_rhs_;

  const std::vector<bool>& is_asc_;
  std::vector<int8_t> nulls_first_;

  using CompareFn = int (*) (const TupleRowComparator*, const TupleRow*, const TupleRow*);
  CompareFn codegend_compare_fn_;

  DISALLOW_COPY_AND_ASSIGN(TupleRowComparator);
};

/// Compares the equality of two Tuples, going slot by slot.
struct TupleEqualityChecker {
  TupleDescriptor* tuple_desc_;

  TupleEqualityChecker(TupleDescriptor* tuple_desc) : tuple_desc_(tuple_desc) {
  }

  bool Equal(Tuple* x, Tuple* y) {
    const std::vector<SlotDescriptor*>& slots = tuple_desc_->slots();
    for (int i = 0; i < slots.size(); ++i) {
      SlotDescriptor* slot = slots[i];

      if (slot->is_nullable()) {
        const NullIndicatorOffset& null_offset = slot->null_indicator_offset();
        if (x->IsNull(null_offset) || y->IsNull(null_offset)) {
          if (x->IsNull(null_offset) && y->IsNull(null_offset)) {
            continue;
          } else {
            return false;
          }
        }
      }

      int tuple_offset = slot->tuple_offset();
      const ColumnType& type = slot->type();
      if (!RawValue::Eq(x->GetSlot(tuple_offset), y->GetSlot(tuple_offset), type)) {
        return false;
      }
    }

    return true;
  }
};

/// Compares the equality of two TupleRows, going tuple by tuple.
struct RowEqualityChecker {
  std::vector<TupleEqualityChecker> tuple_checkers_;

  RowEqualityChecker(const RowDescriptor& row_desc) {
    const std::vector<TupleDescriptor*>& tuple_descs = row_desc.tuple_descriptors();
    for (int i = 0; i < tuple_descs.size(); ++i) {
      tuple_checkers_.push_back(TupleEqualityChecker(tuple_descs[i]));
    }
  }

  bool Equal(const TupleRow* x, const TupleRow* y) {
    for (int i = 0; i < tuple_checkers_.size(); ++i) {
      Tuple* x_tuple = x->GetTuple(i);
      Tuple* y_tuple = y->GetTuple(i);
      if (!tuple_checkers_[i].Equal(x_tuple, y_tuple)) return false;
    }

    return true;
  }
};

}

#endif
