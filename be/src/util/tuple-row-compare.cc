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

#include "util/tuple-row-compare.h"

#include <gutil/strings/substitute.h>

#include "codegen/codegen-anyval.h"
#include "codegen/llvm-codegen.h"
#include "exprs/scalar-expr.h"
#include "exprs/scalar-expr-evaluator.h"
#include "runtime/runtime-state.h"
#include "util/runtime-profile-counters.h"

using namespace impala;
using namespace strings;

Status TupleRowComparator::Prepare(ObjectPool* pool, RuntimeState* state,
    MemPool* expr_perm_pool, MemPool* expr_results_pool) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(ordering_exprs_, state, pool,
      expr_perm_pool, expr_results_pool, &ordering_expr_evals_lhs_));
  RETURN_IF_ERROR(ScalarExprEvaluator::Create(ordering_exprs_, state, pool,
      expr_perm_pool, expr_results_pool, &ordering_expr_evals_rhs_));
  DCHECK_EQ(ordering_exprs_.size(), ordering_expr_evals_lhs_.size());
  DCHECK_EQ(ordering_exprs_.size(), ordering_expr_evals_rhs_.size());
  return Status::OK();
}


Status TupleRowComparator::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(ordering_expr_evals_lhs_, state));
  RETURN_IF_ERROR(ScalarExprEvaluator::Open(ordering_expr_evals_rhs_, state));
  return Status::OK();
}

void TupleRowComparator::Close(RuntimeState* state) {
  ScalarExprEvaluator::Close(ordering_expr_evals_lhs_, state);
  ScalarExprEvaluator::Close(ordering_expr_evals_rhs_, state);
}

int TupleRowComparator::CompareInterpreted(
    const TupleRow* lhs, const TupleRow* rhs) const {
  DCHECK_EQ(ordering_exprs_.size(), ordering_expr_evals_lhs_.size());
  for (int i = 0; i < ordering_exprs_.size(); ++i) {
    void* lhs_value = ordering_expr_evals_lhs_[i]->GetValue(lhs);
    void* rhs_value = ordering_expr_evals_rhs_[i]->GetValue(rhs);

    // The sort order of NULLs is independent of asc/desc.
    if (lhs_value == nullptr && rhs_value == nullptr) continue;
    if (lhs_value == nullptr) return nulls_first_[i];
    if (rhs_value == nullptr) return -nulls_first_[i];

    int result = RawValue::Compare(lhs_value, rhs_value, ordering_exprs_[i]->type());
    if (!is_asc_[i]) result = -result;
    if (result != 0) return result;
    // Otherwise, try the next Expr
  }
  return 0; // fully equivalent key
}

Status TupleRowComparator::Codegen(RuntimeState* state) {
  llvm::Function* fn;
  LlvmCodeGen* codegen = state->codegen();
  DCHECK(codegen != NULL);
  RETURN_IF_ERROR(CodegenCompare(codegen, &fn));
  codegen->AddFunctionToJit(fn, reinterpret_cast<void**>(&codegend_compare_fn_));
  return Status::OK();
}


// Codegens an unrolled version of CompareInterpreted(). Uses codegen'd key exprs and
// injects nulls_first_ and is_asc_ values.
//
// Example IR for comparing an int column then a float column:
//
// define i32 @Compare(%"struct.impala::TupleRowComparator"* %this,
//     %"class.impala::TupleRow"* %lhs, %"class.impala::TupleRow"* %rhs) #40 {
// entry:
// %0 = alloca float
// %1 = alloca float
// %2 = alloca i32
// %3 = alloca i32
// %lhs_value = call i64 @GetSlotRef(%"class.impala::ScalarExprEvaluator"* inttoptr
//     (i64 165283904 to %"class.impala::ScalarExprEvaluator"*),
//     %"class.impala::TupleRow"* %lhs)
// %rhs_value = call i64 @GetSlotRef(%"class.impala::ScalarExprEvaluator"* inttoptr
//     (i64 165284288 to %"class.impala::ScalarExprEvaluator"*),
//     %"class.impala::TupleRow"* %rhs)
// %is_null = trunc i64 %lhs_value to i1
// %is_null1 = trunc i64 %rhs_value to i1
// %both_null = and i1 %is_null, %is_null1
//    br i1 %both_null, label %next_key, label %non_null
//
// non_null:                                         ; preds = %entry
//   br i1 %is_null, label %lhs_null, label %lhs_non_null
//
// lhs_null:                                         ; preds = %non_null
//   ret i32 1
//
// lhs_non_null:                                     ; preds = %non_null
//   br i1 %is_null1, label %rhs_null, label %rhs_non_null
//
// rhs_null:                                         ; preds = %lhs_non_null
//   ret i32 -1
//
// rhs_non_null:                                     ; preds = %lhs_non_null
// %4 = ashr i64 %lhs_value, 32
// %5 = trunc i64 %4 to i32
// store i32 %5, i32* %3
// %6 = bitcast i32* %3 to i8*
// %7 = ashr i64 %rhs_value, 32
// %8 = trunc i64 %7 to i32
// store i32 %8, i32* %2
// %9 = bitcast i32* %2 to i8*
// %result = call i32 @_ZN6impala8RawValue7CompareEPKvS2_RKNS_10ColumnTypeE(
//     i8* %6, i8* %9, %"struct.impala::ColumnType"* @type)
// %10 = icmp ne i32 %result, 0
// br i1 %10, label %result_nonzero, label %next_key
//
// result_nonzero:                                   ; preds = %rhs_non_null
//   ret i32 %result
//
// next_key:                                         ; preds = %rhs_non_null, %entry
// %lhs_value3 = call i64 @GetSlotRef.1(%"class.impala::ScalarExprEvaluator"* inttoptr
//     (i64 165284096 to %"class.impala::ScalarExprEvaluator"*),
//     %"class.impala::TupleRow"* %lhs)
// %rhs_value4 = call i64 @GetSlotRef.1(%"class.impala::ScalarExprEvaluator"* inttoptr
//     (i64 165284480 to %"class.impala::ScalarExprEvaluator"*),
//     %"class.impala::TupleRow"* %rhs)
// %is_null5 = trunc i64 %lhs_value3 to i1
// %is_null6 = trunc i64 %rhs_value4 to i1
// %both_null7 = and i1 %is_null5, %is_null6
// br i1 %both_null7, label %next_key2, label %non_null8
//
// non_null8:                                        ; preds = %next_key
//   br i1 %is_null5, label %lhs_null9, label %lhs_non_null10
//
// lhs_null9:                                        ; preds = %non_null8
//   ret i32 1
//
// lhs_non_null10:                                   ; preds = %non_null8
//   br i1 %is_null6, label %rhs_null11, label %rhs_non_null12
//
// rhs_null11:                                       ; preds = %lhs_non_null10
//   ret i32 -1
//
// rhs_non_null12:                                   ; preds = %lhs_non_null10
// %11 = ashr i64 %lhs_value3, 32
// %12 = trunc i64 %11 to i32
// %13 = bitcast i32 %12 to float
// store float %13, float* %1
// %14 = bitcast float* %1 to i8*
// %15 = ashr i64 %rhs_value4, 32
// %16 = trunc i64 %15 to i32
// %17 = bitcast i32 %16 to float
// store float %17, float* %0
// %18 = bitcast float* %0 to i8*
// %result13 = call i32 @_ZN6impala8RawValue7CompareEPKvS2_RKNS_10ColumnTypeE(
//     i8* %14, i8* %18, %"struct.impala::ColumnType"* @type.2)
// %19 = icmp ne i32 %result13, 0
// br i1 %19, label %result_nonzero14, label %next_key2
//
// result_nonzero14:                                 ; preds = %rhs_non_null12
//   ret i32 %result13
//
// next_key2:                                        ; preds = %rhs_non_null12, %next_key
//   ret i32 0
// }
Status TupleRowComparator::CodegenCompare(LlvmCodeGen* codegen, llvm::Function** fn) {
  llvm::LLVMContext& context = codegen->context();
  const vector<ScalarExpr*>& ordering_exprs = ordering_exprs_;
  llvm::Function* key_fns[ordering_exprs.size()];
  for (int i = 0; i < ordering_exprs.size(); ++i) {
    Status status = ordering_exprs[i]->GetCodegendComputeFn(codegen, &key_fns[i]);
    if (!status.ok()) {
      return Status::Expected(Substitute(
            "Could not codegen TupleRowComparator::Compare(): $0", status.GetDetail()));
    }
  }

  // Construct function signature):
  // int Compare(TupleRowComparator* this, TupleRow* lhs, TupleRow* rhs)
  llvm::PointerType* tuple_row_type = codegen->GetStructPtrType<TupleRow>();
  LlvmCodeGen::FnPrototype prototype(codegen, "Compare", codegen->i32_type());
  // 'this' is used to comply with the signature of CompareInterpreted() and is not used.
  prototype.AddArgument("this", codegen->GetStructPtrType<TupleRowComparator>());
  prototype.AddArgument("lhs", tuple_row_type);
  prototype.AddArgument("rhs", tuple_row_type);

  LlvmBuilder builder(context);
  llvm::Value* args[3];
  *fn = prototype.GeneratePrototype(&builder, args);
  llvm::Value* lhs_arg = args[1];
  llvm::Value* rhs_arg = args[2];

  llvm::PointerType* expr_eval_type = codegen->GetStructPtrType<ScalarExprEvaluator>();

  // Unrolled loop over each key expr
  for (int i = 0; i < ordering_exprs.size(); ++i) {
    // The start of the next key expr after this one. Used to implement "continue" logic
    // in the unrolled loop.
    llvm::BasicBlock* next_key_block = llvm::BasicBlock::Create(context, "next_key", *fn);
    llvm::Value* eval_lhs = codegen->CastPtrToLlvmPtr(expr_eval_type,
        ordering_expr_evals_lhs_[i]);
    llvm::Value* eval_rhs = codegen->CastPtrToLlvmPtr(expr_eval_type,
        ordering_expr_evals_rhs_[i]);
    // Call key_fns[i](ordering_expr_evals_[i], lhs_arg)
    CodegenAnyVal lhs_value = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        ordering_exprs[i]->type(), key_fns[i], {eval_lhs, lhs_arg}, "lhs_value");
    // Call key_fns[i](ordering_expr_evals_[i], rhs_arg)
    CodegenAnyVal rhs_value = CodegenAnyVal::CreateCallWrapped(codegen, &builder,
        ordering_exprs[i]->type(), key_fns[i], {eval_rhs, rhs_arg}, "rhs_value");

    // Handle nullptrs if necessary
    llvm::Value* lhs_null = lhs_value.GetIsNull();
    llvm::Value* rhs_null = rhs_value.GetIsNull();
    // if (lhs_value == nullptr && rhs_value == nullptr) continue;
    llvm::Value* both_null = builder.CreateAnd(lhs_null, rhs_null, "both_null");
    llvm::BasicBlock* non_null_block =
        llvm::BasicBlock::Create(context, "non_null", *fn, next_key_block);
    builder.CreateCondBr(both_null, next_key_block, non_null_block);
    // if (lhs_value == nullptr && rhs_value != nullptr) return nulls_first_[i];
    builder.SetInsertPoint(non_null_block);
    llvm::BasicBlock* lhs_null_block =
        llvm::BasicBlock::Create(context, "lhs_null", *fn, next_key_block);
    llvm::BasicBlock* lhs_non_null_block =
        llvm::BasicBlock::Create(context, "lhs_non_null", *fn, next_key_block);
    builder.CreateCondBr(lhs_null, lhs_null_block, lhs_non_null_block);
    builder.SetInsertPoint(lhs_null_block);
    builder.CreateRet(builder.getInt32(nulls_first_[i]));
    // if (lhs_value != nullptr && rhs_value == nullptr) return -nulls_first_[i];
    builder.SetInsertPoint(lhs_non_null_block);
    llvm::BasicBlock* rhs_null_block =
        llvm::BasicBlock::Create(context, "rhs_null", *fn, next_key_block);
    llvm::BasicBlock* rhs_non_null_block =
        llvm::BasicBlock::Create(context, "rhs_non_null", *fn, next_key_block);
    builder.CreateCondBr(rhs_null, rhs_null_block, rhs_non_null_block);
    builder.SetInsertPoint(rhs_null_block);
    builder.CreateRet(builder.getInt32(-nulls_first_[i]));

    // int result = RawValue::Compare(lhs_value, rhs_value, <type>)
    builder.SetInsertPoint(rhs_non_null_block);
    llvm::Value* result = lhs_value.Compare(&rhs_value, "result");

    // if (!is_asc_[i]) result = -result;
    if (!is_asc_[i]) result = builder.CreateSub(builder.getInt32(0), result, "result");
    // if (result != 0) return result;
    // Otherwise, try the next Expr
    llvm::Value* result_nonzero = builder.CreateICmpNE(result, builder.getInt32(0));
    llvm::BasicBlock* result_nonzero_block =
        llvm::BasicBlock::Create(context, "result_nonzero", *fn, next_key_block);
    builder.CreateCondBr(result_nonzero, result_nonzero_block, next_key_block);
    builder.SetInsertPoint(result_nonzero_block);
    builder.CreateRet(result);

    // Get builder ready for next iteration or final return
    builder.SetInsertPoint(next_key_block);
  }
  builder.CreateRet(builder.getInt32(0));
  *fn = codegen->FinalizeFunction(*fn);
  if (*fn == nullptr) {
    return Status("Codegen'd TupleRowComparator::Compare() function failed verification, "
        "see log");
  }
  return Status::OK();
}

const char* TupleRowComparator::LLVM_CLASS_NAME = "struct.impala::TupleRowComparator";
