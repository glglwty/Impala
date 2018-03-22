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

package org.apache.impala.analysis;

import org.apache.impala.common.AnalysisException;
import org.apache.impala.thrift.TExprNode;

import com.google.common.base.Function;
import java.util.Collections;

/**
 * An expr representing a percentile_disc call in a select stmt. This expr will be
 * rewritten to an analytic function in an inline view after analysis.
 */
public class PercentileAggExpr extends FunctionCallExpr {
  private Expr percentileExpr_;
  private final boolean isAsc_;

  public PercentileAggExpr(Expr percentileExpr, Expr orderByExpr, boolean isAsc) {
    super("percentile_disc", Collections.singletonList(orderByExpr));
    this.percentileExpr_ = percentileExpr;
    this.isAsc_ = isAsc;
  }

  public PercentileAggExpr(PercentileAggExpr other) {
    super(other);
    this.percentileExpr_ = other.percentileExpr_.clone();
    this.isAsc_ = other.isAsc_;
  }

  public boolean isAsc() { return isAsc_; }

  @Override
  public boolean localEquals(Expr that) {
    if (!super.localEquals(that)) return false;
    PercentileAggExpr e = (PercentileAggExpr) that;
    return percentileExpr_.equals(e.percentileExpr_) && isAsc_ == e.isAsc_;
  }

  @Override
  public String toSqlImpl(ToSqlOptions options) {
    return "percentile_disc(" + percentileExpr().toSql(options) +
        ") within group (order by " + orderByExpr().toSql(options) + ")";
  }

  @Override
  protected void toThrift(TExprNode msg) {
    throw new IllegalStateException(
        "PercentileAggExpr should be rewritten without being passed to BE");
  }

  @Override
  protected void analyzeImpl(Analyzer analyzer) throws AnalysisException {
    super.analyzeImpl(analyzer);
    percentileExpr_.analyze(analyzer);
    if (!percentileExpr_.type_.isNumericType()) {
      throw new AnalysisException(
          "Percentile expr is not numeric type : " + percentileExpr_.toSql());
    }
    if (percentileExpr_.contains(Expr.isAggregatePredicate())) {
      throw new AnalysisException(
          "aggregate function must not contain aggregate parameters: " + this.toSql());
    }
    analyzer.setContainsPercentile();
  }

  @Override
  public Expr clone() { return new PercentileAggExpr(this); }

  @Override
  protected Expr substituteImpl(Function<Expr, Expr> f) {
    Expr expr = super.substituteImpl(f);
    if (!(expr instanceof PercentileAggExpr)) return expr;
    PercentileAggExpr e = (PercentileAggExpr) expr;
    e.percentileExpr_ = e.percentileExpr_.substituteImpl(f);
    return e;
  }

  Expr percentileExpr() { return percentileExpr_; }

  Expr orderByExpr() { return getChild(0); }
}
