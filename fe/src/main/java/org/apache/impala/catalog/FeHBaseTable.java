package org.apache.impala.catalog;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TResultSet;

import java.io.IOException;

public interface FeHBaseTable extends FeTable {
  org.apache.hadoop.hbase.client.Table getHBaseTable() throws IOException;

  Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey, byte[] endRowKey);

  String getHBaseTableName();

  TResultSet getTableStats();

  HColumnDescriptor[] getColumnFamilies() throws IOException;
}
