package org.apache.impala.catalog;

import com.google.common.base.Ticker;
import com.sun.management.GarbageCollectionNotificationInfo;
import org.apache.impala.common.Reference;
import org.apache.impala.thrift.TTableName;
import org.apache.log4j.Logger;

import javax.management.Notification;
import javax.management.NotificationEmitter;
import javax.management.NotificationListener;
import javax.management.openmbean.CompositeData;
import java.lang.management.GarbageCollectorMXBean;
import java.lang.management.MemoryUsage;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

/**
 * Automatically invalidates recently unused table. There are currently 2 rules
 * implemented:
 * 1. 10% least recently used tables are invalidated after a GC with a 70% full old
 * generation. The fullness of the GC generation depends on the maximum heap size.
 * 2. If unused_tbl_ttl_sec is set in the backend, unused tables older than the threshold
 * are invalidated periodically.
 */
public class CatalogdTableShrinker {
  public static final Logger LOG = Logger.getLogger(CatalogdTableShrinker.class);
  // Plugable time source for tests. Defined as static to avoid passing
  // CatalogdTableShrinker everywhere the clock is used.
  static Ticker timeSource_ = Ticker.systemTicker();
  private CatalogServiceCatalog catalog_;
  // A thread waking up periodically to check if shrinking is needed.
  private Thread timerThread_;

  private boolean gcTrigger_ = false;
  private long unusedTableTtlSec_;
  private boolean stopped_ = false;
  // A callback called after a shrinking pass is triggered. Used for test purposes.
  private SimpleCallback on_trigger_ = null;

  CatalogdTableShrinker(CatalogServiceCatalog catalog, long unusedTableTtlSec,
      boolean invalidateTableOnMemoryPressure) {
    catalog_ = catalog;
    unusedTableTtlSec_ = unusedTableTtlSec;
    if (invalidateTableOnMemoryPressure) {
      List<GarbageCollectorMXBean> gcbeans =
          java.lang.management.ManagementFactory.getGarbageCollectorMXBeans();

      for (GarbageCollectorMXBean gcbean : gcbeans) {
        NotificationEmitter ne = (NotificationEmitter) gcbean;
        ne.addNotificationListener(new NotificationListener() {
          @Override
          public void handleNotification(Notification notification, Object handback) {
            if (!notification.getType().equals(
                GarbageCollectionNotificationInfo.GARBAGE_COLLECTION_NOTIFICATION))
              return;
            GarbageCollectionNotificationInfo info = GarbageCollectionNotificationInfo
                .from((CompositeData) notification.getUserData());
            if (!info.getGcAction().equals("end of major GC")) return;
            Map<String, MemoryUsage> mem = info.getGcInfo().getMemoryUsageAfterGc();
            MemoryUsage tenuredGenUsage = mem.get("PS Old Gen");
            if (tenuredGenUsage.getMax() * 7 / 10 < tenuredGenUsage.getUsed()) {
              synchronized (CatalogdTableShrinker.this) {
                gcTrigger_ = true;
                CatalogdTableShrinker.this.notify();
              }
            }
          }
        }, null, null);
      }
    }

    timerThread_ = new Thread(new Runnable() {
      @Override
      public void run() {
        while (true) {
          boolean gcTrigger = false;
          synchronized (CatalogdTableShrinker.this) {
            try {
              if (unusedTableTtlSec_ > 0) {
                long wakeUpTimeNano =
                    timeSource_.read() + unusedTableTtlSec_ * 1000 * 1000 * 1000;
                while (!gcTrigger_ && timeSource_.read() < wakeUpTimeNano && !stopped_) {
                  CatalogdTableShrinker.this.wait(unusedTableTtlSec_ * 1000);
                }
              } else {
                while (!gcTrigger_ && !stopped_) CatalogdTableShrinker.this.wait();
              }
              if (gcTrigger_) {
                gcTrigger = true;
                gcTrigger_ = false;
              }
            } catch (InterruptedException e) {
              // do nothing
            }
            if (stopped_) return;
            if (gcTrigger) invalidate10percent();
            if (unusedTableTtlSec_ > 0) invalidateOlderThan(unusedTableTtlSec_);
          }
          if (on_trigger_ != null) on_trigger_.call();
        }
      }
    });
    timerThread_.start();
  }

  static long nanoTime() {
    return timeSource_.read();
  }

  private void invalidate10percent() {
    List<Table> tables = new ArrayList<>();
    for (Db db : catalog_.getAllDbs()) {
      for (Table table : db.getTables()) {
        if (table.isLoaded()) tables.add(table);
      }
    }
    // TODO: use quick select
    Collections.sort(tables, new Comparator<Table>() {
      @Override
      public int compare(Table o1, Table o2) {
        return Long.compare(o1.getLastUsedTime(), o2.getLastUsedTime());
      }
    });
    for (int i = 0; i < tables.size() / 10; ++i) {
      TTableName tTableName = tables.get(i).getTableName().toThrift();
      Reference<Boolean> tblWasRemoved = new Reference<>();
      Reference<Boolean> dbWasAdded = new Reference<>();
      catalog_.invalidateTable(tTableName, tblWasRemoved, dbWasAdded);
    }
  }

  private void invalidateOlderThan(long retireAgeSecs) {
    long now = timeSource_.read();
    for (Db db : catalog_.getAllDbs()) {
      for (Table table : catalog_.getAllTables(db)) {
        if (table.isLoaded() &&
            now - table.getLastUsedTime() > retireAgeSecs * 1000000000L) {
          Reference<Boolean> tblWasRemoved = new Reference<>();
          Reference<Boolean> dbWasAdded = new Reference<>();
          TTableName tTableName = table.getTableName().toThrift();
          catalog_.invalidateTable(tTableName, tblWasRemoved, dbWasAdded);
        }
      }
    }
  }

  void setOnTrigger(SimpleCallback callback) {
    on_trigger_ = callback;
  }

  void stop() throws InterruptedException {
    synchronized (this) {
      stopped_ = true;
      notify();
    }
    timerThread_.join();
  }

  interface SimpleCallback {
    void call();
  }
}
