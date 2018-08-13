package org.apache.impala.catalog;

import org.apache.impala.analysis.TableName;
import org.apache.impala.service.FeSupport;
import org.apache.impala.thrift.TTableName;
import org.apache.impala.thrift.TTableUsage;
import org.apache.impala.thrift.TUpdateUsedTableNamesRequest;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;

import java.util.Collection;
import java.util.HashMap;

/**
 * Report table usage to catalogd.
 */
public class ImpaladTableUsageTracker {
  private static final Logger LOG = Logger.getLogger(ImpaladCatalog.class);
  public static ImpaladTableUsageTracker INSTANCE;
  private TUpdateUsedTableNamesRequest req;
  private Thread reportThread;

  public static void create() {
    INSTANCE = new ImpaladTableUsageTracker();
  }

  private ImpaladTableUsageTracker() {
    req = new TUpdateUsedTableNamesRequest(new HashMap<TTableName, TTableUsage>());
    reportThread = new Thread(new Runnable() {
      @Override
      public void run() {
        report();
      }
    });
    reportThread.start();
  }

  /**
   * Report used table names asynchronously.
   */
  public synchronized void addTables(Collection<TableName> tableNames) {
    for (TableName tableName : tableNames) {
      TTableName tTableName = tableName.toThrift();
      if (req.tables.containsKey(tTableName)) {
        req.tables.get(tTableName).num_usage++;
      } else {
        req.tables.put(tTableName, new TTableUsage(0));
      }
    }
    notify();
  }

  private void report() {
    while (true) {
      try {
        TUpdateUsedTableNamesRequest reqToSend;
        synchronized (this) {
          while (req.tables.isEmpty()) wait();
          reqToSend = req;
          req = new TUpdateUsedTableNamesRequest(new HashMap<TTableName, TTableUsage>());
        }
        FeSupport.NativeReportTableUsage(new TSerializer().serialize(reqToSend));
      } catch (InterruptedException e) {
        // do nothing
      } catch (TException e) {
        LOG.warn("Thrift serialization failure: ", e);
      }
    }
  }

}
