package org.apache.impala.catalog;

import com.google.common.base.Ticker;
import org.apache.impala.common.Reference;
import org.apache.impala.testutil.CatalogServiceTestCatalog;
import org.apache.impala.thrift.TTableName;
import org.junit.Test;

public class CatalogdTableShrinkerTest {
  private CatalogServiceCatalog catalog_ = CatalogServiceTestCatalog.create();

  private void waitForTrigger(final Boolean[] cleanTriggered)
      throws InterruptedException {
    synchronized (cleanTriggered) {
      while (!cleanTriggered[0]) cleanTriggered.wait();
      cleanTriggered[0] = false;
    }
  }

  @Test
  public void testCatalogdTableCleaner() throws CatalogException, InterruptedException {
    Reference<Boolean> tblWasRemoved = new Reference<>();
    Reference<Boolean> dbWasAdded = new Reference<>();
    String dbName = "functional";
    String tblName = "alltypes";
    catalog_.invalidateTable(new TTableName("functional", "alltypes"), tblWasRemoved,
        dbWasAdded);
    MockTicker ticker = new MockTicker();
    CatalogdTableShrinker.timeSource_ = ticker;
    catalog_.setCatalogdTableShrinker(new CatalogdTableShrinker(catalog_, 2, false));
    assert !catalog_.getDb(dbName).getTable(tblName).isLoaded();
    Table table = catalog_.getOrLoadTable(dbName, tblName);
    assert table.isLoaded();
    assert table.getLastUsedTime() == 0;
    final Boolean[] cleanTriggered = {false};
    // Get notified when the shrinking is done and proceed with the tests.
    catalog_.getCatalogdTableShrinker()
        .setOnTrigger(new CatalogdTableShrinker.SimpleCallback() {
          @Override
          public void call() {
            synchronized (cleanTriggered) {
              cleanTriggered[0] = true;
              cleanTriggered.notify();
            }
          }
        });
    long nanoSecsPerSec = 1000L * 1000 * 1000;
    ticker.set(nanoSecsPerSec);
    table.refreshLastUsedTime();
    ticker.set(3 * nanoSecsPerSec);
    waitForTrigger(cleanTriggered);
    // The last used time is refreshed so the table won't be invalidated
    assert catalog_.getTable(dbName, tblName).isLoaded();
    ticker.set(6 * nanoSecsPerSec);
    waitForTrigger(cleanTriggered);
    // The table is now invalidated
    assert !catalog_.getTable(dbName, tblName).isLoaded();
    catalog_.getCatalogdTableShrinker().stop();
    catalog_.setCatalogdTableShrinker(null);
  }

  class MockTicker extends Ticker {
    long now_ = 0;

    @Override
    synchronized public long read() {
      return now_;
    }

    void set(long nanoSec) {
      synchronized (this) {
        now_ = nanoSec;
      }
      CatalogdTableShrinker cleaner = catalog_.getCatalogdTableShrinker();
      synchronized (cleaner) {
        cleaner.notify();
      }
    }
  }
}
