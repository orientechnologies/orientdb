/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.concur.executors;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Sitnikov
 */
public class SubScheduledExecutorServiceTest {

  private ScheduledThreadPoolExecutor executor;
  private ScheduledExecutorService    subExecutor;

  @Before
  public void before() {
    executor = new ScheduledThreadPoolExecutor(1);
    subExecutor = new SubScheduledExecutorService(executor);
  }

  @After
  public void after() throws InterruptedException {
    subExecutor.shutdown();
    subExecutor.awaitTermination(5, TimeUnit.SECONDS);
    assertTrue(subExecutor.isTerminated());

    executor.shutdown();
    executor.awaitTermination(5, TimeUnit.SECONDS);
    assertTrue(executor.isTerminated());
  }

  @Test
  public void testScheduleRunnable() throws ExecutionException, InterruptedException {
    final long start = System.currentTimeMillis();

    final AtomicBoolean ran100 = new AtomicBoolean(false);
    final ScheduledFuture<?> future100 = subExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        ran100.set(true);
      }
    }, 100, TimeUnit.MILLISECONDS);

    final AtomicBoolean ran200 = new AtomicBoolean(false);
    final ScheduledFuture<?> future200 = subExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        ran200.set(true);
      }
    }, 200, TimeUnit.MILLISECONDS);

    final AtomicBoolean ran50 = new AtomicBoolean(false);
    final ScheduledFuture<?> future50 = subExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        ran50.set(true);
      }
    }, 50, TimeUnit.MILLISECONDS);

    future50.get();
    assertTrue(fuzzyGreater(span(start), 50));
    assertTrue(ran50.get());

    future100.get();
    assertTrue(fuzzyGreater(span(start), 100));
    assertTrue(ran100.get());

    future200.get();
    assertTrue(fuzzyGreater(span(start), 200));
    assertTrue(ran200.get());
  }

  @Test
  public void testScheduleCallable() throws ExecutionException, InterruptedException {
    final long start = System.currentTimeMillis();

    final ScheduledFuture<Boolean> future100 = subExecutor.schedule(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return true;
      }
    }, 100, TimeUnit.MILLISECONDS);

    final ScheduledFuture<Boolean> future200 = subExecutor.schedule(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return true;
      }
    }, 200, TimeUnit.MILLISECONDS);

    final ScheduledFuture<Boolean> future50 = subExecutor.schedule(new Callable<Boolean>() {
      @Override
      public Boolean call() {
        return true;
      }
    }, 50, TimeUnit.MILLISECONDS);

    assertTrue(future50.get());
    assertTrue(fuzzyGreater(span(start), 50));

    assertTrue(future100.get());
    assertTrue(fuzzyGreater(span(start), 100));

    assertTrue(future200.get());
    assertTrue(fuzzyGreater(span(start), 200));
  }

  @Test
  public void testScheduleAtFixedRate() throws Exception {
    final long start = System.currentTimeMillis();

    final AtomicInteger counter = new AtomicInteger(0);
    subExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
        counter.incrementAndGet();
      }
    }, 50, 50, TimeUnit.MILLISECONDS);

    assertTrue(busyWait(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return counter.get() >= 5;
      }
    }));

    assertTrue(fuzzyGreater(span(start), 5 * 50));
  }

  @Test
  public void testScheduleWithFixedDelay() throws Exception {
    final long start = System.currentTimeMillis();

    final AtomicInteger counter = new AtomicInteger(0);
    subExecutor.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        counter.incrementAndGet();
      }
    }, 50, 50, TimeUnit.MILLISECONDS);

    assertTrue(busyWait(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return counter.get() >= 5;
      }
    }));

    assertTrue(fuzzyGreater(span(start), 5 * 50));
  }

  @Test(expected = CancellationException.class)
  public void testCancelDelayed() throws InterruptedException, ExecutionException {
    final long start = System.currentTimeMillis();

    final ScheduledFuture<Boolean> future = subExecutor.schedule(new Callable<Boolean>() {
      @Override
      public Boolean call() throws Exception {
        return true;
      }
    }, 200, TimeUnit.MILLISECONDS);

    future.cancel(false);
    subExecutor.shutdown();
    subExecutor.awaitTermination(5, TimeUnit.SECONDS);

    assertTrue(fuzzyLess(span(start), 200));

    future.get();
  }

  @Test(expected = CancellationException.class)
  public void testCancelPeriodic() throws InterruptedException, ExecutionException {
    final long start = System.currentTimeMillis();

    final ScheduledFuture<?> future = subExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
      }
    }, 0, 200, TimeUnit.MILLISECONDS);

    future.cancel(false);
    subExecutor.shutdown();
    subExecutor.awaitTermination(5, TimeUnit.SECONDS);

    assertTrue(fuzzyLess(span(start), 200));

    future.get();
  }

  @Test
  public void testShutdown() throws Exception {
    final long start = System.currentTimeMillis();

    final AtomicBoolean delayedRan = new AtomicBoolean(false);

    subExecutor.schedule(new Runnable() {
      @Override
      public void run() {
        delayedRan.set(true);
      }
    }, 100, TimeUnit.MILLISECONDS);

    subExecutor.scheduleAtFixedRate(new Runnable() {
      @Override
      public void run() {
      }
    }, 0, 20, TimeUnit.MILLISECONDS);

    subExecutor.shutdown();
    assertTrue(subExecutor.isShutdown());
    assertFalse(subExecutor.isTerminated());
    assertFalse(delayedRan.get());

    assertTrue(subExecutor.awaitTermination(5, TimeUnit.SECONDS));
    assertTrue(delayedRan.get());
    assertTrue(fuzzyGreater(span(start), 100));

    assertTrue(subExecutor.isShutdown());
    assertTrue(subExecutor.isTerminated());
    assertFalse(executor.isShutdown());
    assertFalse(executor.isTerminated());
  }

  private static long span(long start) {
    return System.currentTimeMillis() - start;
  }

  private static boolean fuzzyGreater(long actual, long expected) {
    return actual > expected - (expected / 4.0); // 25% tolerance
  }

  private static boolean fuzzyLess(long actual, long expected) {
    return actual < expected + (expected / 4.0); // 25% tolerance
  }

  private static boolean busyWait(Callable<Boolean> condition) throws Exception {
    if (condition.call())
      return true;

    final long start = System.currentTimeMillis();
    do {
      Thread.sleep(20);

      if (condition.call())
        return true;

      if (System.currentTimeMillis() - start >= 5000)
        return false;
    } while (true);
  }

}
