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

package org.apache.impala.catalog;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.impala.common.InternalException;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TCatalogInfoSelector;
import org.apache.impala.thrift.TCatalogObject;
import org.apache.impala.thrift.TCatalogObjectType;
import org.apache.impala.thrift.TDatabase;
import org.apache.impala.thrift.TDbInfoSelector;
import org.apache.impala.thrift.TGetPartialCatalogObjectRequest;
import org.apache.impala.thrift.TGetPartialCatalogObjectResponse;
import org.apache.impala.thrift.TPartialPartitionInfo;
import org.apache.impala.thrift.TTable;
import org.apache.impala.thrift.TTableInfoSelector;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class PartialCatalogInfoTest {
  private static CatalogServiceCatalog catalog_ =
      CatalogServiceTestCatalog.create();

  private TGetPartialCatalogObjectResponse sendRequest(
      TGetPartialCatalogObjectRequest req)
      throws CatalogException, InternalException, TException {
    System.err.println("req: " + req);
    TGetPartialCatalogObjectResponse resp;
    resp = catalog_.getPartialCatalogObject(req);
    // Round-trip the response through serialization, so if we accidentally forgot to
    // set the "isset" flag for any fields, we'll catch that bug.
    byte[] respBytes = new TSerializer().serialize(resp);
    resp.clear();
    new TDeserializer().deserialize(resp, respBytes);
    System.err.println("resp: " + resp);
    return resp;
  }

  @Test
  public void testDbList() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.CATALOG);
    req.catalog_info_selector = new TCatalogInfoSelector();
    req.catalog_info_selector.want_db_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertTrue(resp.catalog_info.db_names.contains("functional"));
  }

  @Test
  public void testDb() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.DATABASE);
    req.object_desc.db = new TDatabase("functional");
    req.db_info_selector = new TDbInfoSelector();
    req.db_info_selector.want_hms_database = true;
    req.db_info_selector.want_table_names = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertTrue(resp.isSetObject_version_number());
    assertEquals(resp.db_info.hms_database.getName(), "functional");
    assertTrue(resp.db_info.table_names.contains("alltypes"));
  }

  @Test
  public void testTable() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "alltypes");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_hms_table = true;
    req.table_info_selector.want_partition_list = true;
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    assertTrue(resp.isSetObject_version_number());
    assertEquals(resp.table_info.hms_table.getTableName(), "alltypes");
    assertTrue(resp.table_info.partitions.size() > 0);
    TPartialPartitionInfo partInfo = resp.table_info.partitions.get(1);
    assertEquals("year=2009/month=10", partInfo.name);

    // Fetch again, but specify two specific partitions and ask for metadata.
    req.table_info_selector.clear();
    req.table_info_selector.want_partition_metadata = true;
    req.table_info_selector.partition_ids = ImmutableList.of(
        resp.table_info.partitions.get(1).id,
        resp.table_info.partitions.get(3).id);
    resp = catalog_.getPartialCatalogObject(req);
    assertNull(resp.table_info.hms_table);
    assertEquals(2, resp.table_info.partitions.size());
    partInfo = resp.table_info.partitions.get(0);
    assertNull(partInfo.name);
    assertEquals(req.table_info_selector.partition_ids.get(0), (Long)partInfo.id);
    assertTrue(partInfo.hms_partition.getSd().getLocation().startsWith(
        "hdfs://localhost:20500/test-warehouse/alltypes/year="));
    // TODO(todd): we should probably transfer a compressed descriptor instead
    // and refactor the MetaProvider interface to expose those since there is
    // a lot of redundant info in partition descriptors.
    // TODO(todd): should also filter out the incremental stats.
  }

  @Test
  public void testTableStats() throws Exception {
    TGetPartialCatalogObjectRequest req = new TGetPartialCatalogObjectRequest();
    req.object_desc = new TCatalogObject();
    req.object_desc.setType(TCatalogObjectType.TABLE);
    req.object_desc.table = new TTable("functional", "alltypes");
    req.table_info_selector = new TTableInfoSelector();
    req.table_info_selector.want_stats_for_column_names = ImmutableList.of(
        "year", "month", "day", "id", "bool_col", "tinyint_col", "smallint_col",
        "int_col", "bigint_col", "float_col", "double_col", "date_string_col",
        "string_col", "timestamp_col");
    TGetPartialCatalogObjectResponse resp = sendRequest(req);
    List<ColumnStatisticsObj> stats = resp.table_info.column_stats;
    assertEquals(13, stats.size());
    assertEquals("ColumnStatisticsObj(colName:year, colType:INT, " +
        "statsData:<ColumnStatisticsData longStats:LongColumnStatsData(" +
        "numNulls:-1, numDVs:-1)>)", stats.get(0).toString());
  }
}
