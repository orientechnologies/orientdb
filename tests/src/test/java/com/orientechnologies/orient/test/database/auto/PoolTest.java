package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 3/31/14
 */
@Test
public class PoolTest extends DocumentDBBaseTest {
  private int counter = 0;
  private final Object lock = new Object();

  private final CountDownLatch latch = new CountDownLatch(1);

  @Parameters(value = "url")
  public PoolTest(@Optional String url) {
    super(url);
  }

  public void testPoolSize() throws Exception {
    ExecutorService executorService = Executors.newCachedThreadPool();

    final int maxSize = OGlobalConfiguration.DB_POOL_MAX.getValueAsInteger();
    final Random random = new Random();

    final List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int i = 0; i < maxSize / 2; i++)
      futures.add(executorService.submit(new Acquirer(maxSize, random)));

    latch.countDown();

    for (Future<Void> future : futures) future.get();
  }

  private final class Acquirer implements Callable<Void> {
    private final int maxSize;
    private final Random random;

    private Acquirer(int maxSize, Random random) {
      this.maxSize = maxSize;
      this.random = random;
    }

    @Override
    public Void call() throws Exception {
      final int delay = random.nextInt(500) + 200;

      ODatabaseDocumentTx databaseDocumentTx;
      synchronized (lock) {
        databaseDocumentTx = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");
        counter++;
      }

      try {
        latch.await();
        Thread.sleep(delay);

        Assert.assertEquals(ODatabaseDocumentPool.global().getMaxSize(), maxSize);

        synchronized (lock) {
          Assert.assertEquals(
              ODatabaseDocumentPool.global().getAvailableConnections(url, "admin"),
              maxSize - counter);
        }
      } finally {
        synchronized (lock) {
          databaseDocumentTx.close();
          counter--;
        }
      }

      return null;
    }
  }
}
