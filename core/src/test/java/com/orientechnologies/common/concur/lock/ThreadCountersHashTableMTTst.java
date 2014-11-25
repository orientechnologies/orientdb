package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.util.MersenneTwister;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.security.SecureRandom;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 8/27/14
 */
@Test(enabled = false)
public class ThreadCountersHashTableMTTst {
  private OThreadCountersHashTable threadCounters;
  private final Queue<Thread>      addedThreads    = new ConcurrentLinkedQueue<Thread>();

  private final ExecutorService    executorService = Executors.newCachedThreadPool();
  private CountDownLatch           latch           = new CountDownLatch(1);
  private volatile boolean         stop            = false;
  private final AtomicLong         threadsCounter  = new AtomicLong();

  public void testAddition() throws Exception {
    final List<Future> futures = new ArrayList<Future>();

    long start = System.currentTimeMillis();

    int n = 0;
    while (System.currentTimeMillis() - start < 60 * 60 * 1000) {
      n++;
      futures.clear();

      addedThreads.clear();
      threadsCounter.set(0);
      stop = false;

      threadCounters = new OThreadCountersHashTable(2, true);
      latch = new CountDownLatch(1);

      System.out.println("Start iteration " + n);
      for (int i = 0; i < 7; i++)
        futures.add(executorService.submit(new Adder()));

      futures.add(executorService.submit(new Checker()));

      latch.countDown();

      Thread.sleep(10 * 1000);

      stop = true;

      for (Future future : futures)
        future.get();

      System.out.println("Iteration : " + n + " Added threads : " + threadsCounter.get());

      for (Thread thread : addedThreads)
        Assert.assertSame(threadCounters.search(thread).getThread(), thread);
    }

  }

  private final class Checker implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      latch.await();

      try {
        while (!stop) {
          for (Thread thread : addedThreads) {
            Assert.assertSame(threadCounters.search(thread).getThread(), thread);
            if (stop)
              return null;
          }

        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return null;
    }
  }

  private final class Adder implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      latch.await();

      try {
        while (!stop) {
          final Thread thread = new Thread();

          threadCounters.insert(thread);
          addedThreads.add(thread);
          threadsCounter.getAndIncrement();
        }
      } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException(e);
      }

      return null;
    }
  }
}
