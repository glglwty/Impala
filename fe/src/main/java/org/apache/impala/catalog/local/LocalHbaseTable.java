package org.apache.impala.catalog.local;

import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.impala.catalog.Column;
import org.apache.impala.catalog.FeCatalogUtils;
import org.apache.impala.catalog.FeHBaseTable;
import org.apache.impala.catalog.HBaseTable;
import org.apache.impala.common.Pair;
import org.apache.impala.thrift.TResultSet;
import org.apache.impala.thrift.TTableDescriptor;
import org.apache.impala.thrift.TTableType;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Set;

public class LocalHbaseTable extends LocalTable implements FeHBaseTable {
  static LocalHbaseTable loadFromHbase(LocalDb db, Table msTable) {
    try {
      // Warm up the connection and verify the table exists.
      HBaseTable.getHBaseTable(HBaseTable.getHBaseTableName(msTable)).close();
      return new LocalHbaseTable(db, msTable, new ColumnMap(
          HBaseTable.loadColumns(msTable), 1, msTable.getDbName() + "." + msTable.getTableName()));
    } catch (Exception e) {
      throw new LocalCatalogException(e);
    }
  }

  private LocalHbaseTable(LocalDb db, Table msTbl, ColumnMap cols) {
    super(db, msTbl, cols);
  }

  @Override
  public TTableDescriptor toThriftDescriptor(int tableId,
      Set<Long> referencedPartitions) {
    TTableDescriptor tableDescriptor =
        new TTableDescriptor(tableId, TTableType.HBASE_TABLE,
            FeCatalogUtils.getTColumnDescriptors(this), 1,
            getHBaseTableName(), db_.getName());
    tableDescriptor.setHbaseTable(HBaseTable.getTHBaseTable(this));
    return tableDescriptor;
  }

  @Override
  public org.apache.hadoop.hbase.client.Table getHBaseTable() throws IOException {
    return HBaseTable.getHBaseTable(getHBaseTableName());
  }

  @Override
  public Pair<Long, Long> getEstimatedRowStats(byte[] startRowKey, byte[] endRowKey) {
    return HBaseTable.getEstimatedRowStats(startRowKey, endRowKey, this);
  }

  @Override
  public String getHBaseTableName() {
    return HBaseTable.getHBaseTableName(msTable_);
  }

  @Override
  public TResultSet getTableStats() {
    return HBaseTable.getTableStats(this);
  }

  @Override
  public HColumnDescriptor[] getColumnFamilies() throws IOException {
    return getHBaseTable().getTableDescriptor().getColumnFamilies();
  }

  @Override
  public ArrayList<Column> getColumnsInHiveOrder() {
    return getColumns();
  }
}
