package com.orientechnologies.orient.core.storage;

import junit.framework.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 15.06.12
 */
@Test
public class StorageModificationLockTest {
  private final static int               THREAD_COUNT     = 100;
	private final static int 							 CYCLES_COUNT     = 20;
  private final AtomicLong               counter          = new AtomicLong();
  private final OStorageModificationLock modificationLock = new OStorageModificationLock();
  private final ExecutorService          executorService  = Executors.newFixedThreadPool(THREAD_COUNT + 1);
  private final List<Future>             futures          = new ArrayList<Future>(THREAD_COUNT);
  private final CountDownLatch           countDownLatch   = new CountDownLatch(1);

  @Test
  public void testLock() throws Exception {
    for (int i = 0; i < THREAD_COUNT; i++) {
      futures.add(executorService.submit(new Counter()));
    }

		Future prohibiter = executorService.submit(new Prohibiter());
    countDownLatch.countDown();
    prohibiter.get();
		for(Future future : futures)
			future.get();
  }

  private final class Counter implements Callable<Void> {
    private final Random random = new Random();

    public Void call() throws Exception {
      countDownLatch.await();
      for (int n = 0; n < CYCLES_COUNT; n++) {
        final int modificationsCount = random.nextInt(255);
        modificationLock.requestModificationLock();
        try {
          for (int i = 0; i < modificationsCount; i++) {
            counter.incrementAndGet();
            Thread.sleep(random.nextInt(5));
          }
        } finally {
          modificationLock.releaseModificationLock();
        }
      }
      return null;
    }
  }

  private final class Prohibiter implements Callable<Void> {
    public Void call() throws Exception {
      countDownLatch.await();

      for (int n = 0; n < CYCLES_COUNT; n++) {
        modificationLock.prohibitModifications();
        long beforeModification = counter.get();
        Thread.sleep(50);
        if (n % 10 == 0)
          System.out
              .println("After prohibit  modifications " + beforeModification + " before allow modifications " + counter.get());
        Assert.assertEquals(beforeModification, counter.get());
        modificationLock.allowModifications();
        Thread.sleep(50);
      }
			return null;
		}
  }
}
