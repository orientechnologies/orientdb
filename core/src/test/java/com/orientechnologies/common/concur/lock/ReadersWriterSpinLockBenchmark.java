package com.orientechnologies.common.concur.lock;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/19/14
 */
public class ReadersWriterSpinLockBenchmark {
  private final OReadersWriterSpinLock spinLock = new OReadersWriterSpinLock();
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private final AtomicLong readLocksCount = new AtomicLong();

  private final AtomicLong acquireLockSum = new AtomicLong();
  private final AtomicLong releaseLockSum = new AtomicLong();

  private final CountDownLatch latch = new CountDownLatch(1);
  private volatile boolean stop = false;

  private volatile long c = 47;

  public void benchmark() throws Exception {
    List<Future> futures = new ArrayList<Future>();

    for (int i = 0; i < 8; i++) futures.add(executorService.submit(new Reader()));

    latch.countDown();

    Thread.sleep(60 * 1000);

    stop = true;

    for (Future future : futures) future.get();

    System.out.println(
        "Average acquire read lock interval : "
            + (acquireLockSum.get() / readLocksCount.get())
            + " ns.");
    System.out.println(
        "Average release read lock interval : "
            + (releaseLockSum.get() / readLocksCount.get())
            + " ns.");
  }

  private void consumeCPU(int cycles) {
    long c1 = c;
    for (int i = 0; i < cycles; i++) {
      c1 += c1 * 31 + i * 51;
    }
    c = c1;
  }

  public final class Reader implements Callable<Void> {

    @Override
    public Void call() throws Exception {
      latch.await();

      while (!stop) {
        long start = System.nanoTime();
        spinLock.acquireReadLock();
        long end = System.nanoTime();

        readLocksCount.incrementAndGet();
        acquireLockSum.addAndGet(end - start);

        consumeCPU(100);

        start = System.nanoTime();
        spinLock.releaseReadLock();
        end = System.nanoTime();

        releaseLockSum.addAndGet(end - start);
      }

      return null;
    }
  }
}
