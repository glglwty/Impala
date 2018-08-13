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
import pytest
import time
import urllib

from tests.common.custom_cluster_test_suite import CustomClusterTestSuite


class TestAutomaticCatalogInvalidation(CustomClusterTestSuite):
    @classmethod
    def get_workload(cls):
        return 'functional-query'

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(catalogd_args="--unused_table_ttl_sec=10")
    def test_v1_catalog(self, cursor):
        """IMPALA-5789: Test that always false filter filters out splits when file-level
        filtering is disabled."""
        query = "select count(*) from functional.alltypes"
        metadata_cache_string = "Metadata of all 1 tables cached"
        cursor.execute(query)
        cursor.fetchall()
        assert metadata_cache_string not in cursor.get_profile()
        cursor.execute(query)
        cursor.fetchall()
        assert metadata_cache_string in cursor.get_profile()
        time.sleep(20)
        cursor.execute(query)
        cursor.fetchall()
        assert metadata_cache_string not in cursor.get_profile()

    @pytest.mark.execute_serially
    @CustomClusterTestSuite.with_args(catalogd_args="--unused_table_ttl_sec=10",
                                      impalad_args="--use_local_catalog")
    def test_local_catalog(self, cursor):
        """IMPALA-5789: Test that always false filter filters out splits when file-level
        filtering is disabled."""
        query = "select count(*) from functional.alltypes"
        metadata_cache_string = "columns (list) = list&lt;struct&gt;"
        url = "http://localhost:25020/catalog_object?object_type=TABLE&" \
              "object_name=functional.alltypes"
        cursor.execute(query)
        cursor.fetchall()
        assert metadata_cache_string in urllib.urlopen(url).read()
        time.sleep(20)
        assert metadata_cache_string not in urllib.urlopen(url).read()

