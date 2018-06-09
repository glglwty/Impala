# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

import os
import re

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestReplaceTupleRowCompare(CustomClusterTestSuite):
  """ Tests that cross-compiled function CompareInterpreted() doesn't present in the
  optimized IR after it's replaced by the codegened Compare(). """
  log_dir = os.getenv('LOG_DIR', "/tmp/")

  @classmethod
  def get_workload(cls):
    return 'functional-query'

  @CustomClusterTestSuite.with_args(
      "-dump_ir -opt_module_dir=\"" + log_dir + "\"")
  def test_replace_tuple_row_compare(self):
    self.client.execute("SET DISABLE_CODEGEN_ROWS_THRESHOLD=0")
    # Sort and topn multiple times to test that the replacement is COW.
    sort_query = "select row_number() over (order by int_col, float_col), row_number() " \
                 "over (order by float_col, int_col) from functional.alltypes"
    topn_query = "select * from (select * from functional.alltypes order by int_col " \
                 "limit 10) v union (select * from functional.alltypes order by " \
                 "float_col limit 10) order by int_col, float_col limit 5"
    for query in [sort_query, topn_query]:
      result = self.client.execute(query)
      assert result.success
      coord_id = re.search("Query \(id=([^)]*)\)", result.runtime_profile).group(1)
      ir = open(os.path.join(self.log_dir, coord_id[:-1] + "1_opt.ll")).read()
      # If CompareInterpreted is inlined, there should still be lables named like
      # "_ZNK6impala18TupleRowComparator18CompareInterpretedEPKNS_8TupleRowES3_.exit"
      # in the IR. So CompareInterpreted not presenting in the text should show that it's
      # replaced.
      assert "Compare" in ir and "CompareInterpreted" not in ir
