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

import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Sitnikov
 */
public class SubExecutorRandomizedTest {

  private static final int TASKS = 3000;
  private static final int TIME  = 1500;

  private static final int CORES = Runtime.getRuntime().availableProcessors();

  private ScheduledThreadPoolExecutor executor;
  private ScheduledExecutorService    subExecutor;

  private Random random;

  @Before
  public void before() {
    executor = new ScheduledThreadPoolExecutor(CORES);
    subExecutor = new SubScheduledExecutorService(executor);

    final long seed = System.currentTimeMillis();
    System.out.println("SubExecutorRandomizedTest seed: " + seed);
    random = new Random(seed);
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
  public void test() throws InterruptedException, ExecutionException {
    final AtomicLong runs = new AtomicLong(0);
    final AtomicLong delayedRuns = new AtomicLong(0);
    final AtomicLong periodicRuns = new AtomicLong(0);

    long expectedRuns = 0;
    long expectedDelayedRuns = 0;
    long expectedPeriodicRuns = 0;

    for (int i = 0; i < TASKS; ++i)
      switch (random.nextInt(11)) {
      case 0:
        subExecutor.execute(new Runnable() {
          @Override
          public void run() {
            runs.incrementAndGet();
          }
        });
        ++expectedRuns;
        break;

      case 1:
        subExecutor.submit(new Runnable() {
          @Override
          public void run() {
            runs.incrementAndGet();
          }
        });
        ++expectedRuns;
        break;

      case 2:
        subExecutor.submit(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            runs.incrementAndGet();
            return null;
          }
        });
        ++expectedRuns;
        break;

      case 3:
        subExecutor.submit(new Runnable() {
          @Override
          public void run() {
            runs.incrementAndGet();
          }
        }, null);
        ++expectedRuns;
        break;

      case 4:
        subExecutor.schedule(new Runnable() {
          @Override
          public void run() {
            delayedRuns.incrementAndGet();
          }
        }, random.nextInt(TIME), TimeUnit.MILLISECONDS);
        ++expectedDelayedRuns;
        break;

      case 5:
        subExecutor.schedule(new Callable<Object>() {
          @Override
          public Object call() throws Exception {
            delayedRuns.incrementAndGet();
            return null;
          }
        }, random.nextInt(TIME), TimeUnit.MILLISECONDS);
        ++expectedDelayedRuns;
        break;

      case 6: {
        final long delay = random.nextInt(TIME);
        final long period = 1 + random.nextInt(TIME);

        subExecutor.scheduleAtFixedRate(new Runnable() {
          @Override
          public void run() {
            periodicRuns.incrementAndGet();
          }
        }, delay, period, TimeUnit.MILLISECONDS);
        expectedPeriodicRuns += 1 + (TIME - delay) / period;
      }
      break;

      case 7: {
        final long delay = random.nextInt(TIME);
        final long period = 1 + random.nextInt(TIME);

        subExecutor.scheduleWithFixedDelay(new Runnable() {
          @Override
          public void run() {
            periodicRuns.incrementAndGet();
          }
        }, delay, period, TimeUnit.MILLISECONDS);
        expectedPeriodicRuns += 1 + (TIME - delay) / period;
      }
      break;

      case 8:
        subExecutor.submit(new Runnable() {
          @Override
          public void run() {
            runs.incrementAndGet();
            subExecutor.submit(new Runnable() {
              @Override
              public void run() {
                runs.incrementAndGet();
              }
            });
          }
        });
        expectedRuns += 2;
        break;

      case 9:
        subExecutor.schedule(new Runnable() {
          @Override
          public void run() {
            delayedRuns.incrementAndGet();
            subExecutor.schedule(new Runnable() {
              @Override
              public void run() {
                delayedRuns.incrementAndGet();
              }
            }, random.nextInt(TIME / 2), TimeUnit.MILLISECONDS);
          }
        }, random.nextInt(TIME / 2), TimeUnit.MILLISECONDS);
        expectedDelayedRuns += 2;
        break;

      case 10: {
        final long delay = random.nextInt(TIME / 2);
        final long subDelay = random.nextInt(TIME / 2);
        final long subPeriod = 1 + random.nextInt(TIME / 2);

        subExecutor.schedule(new Runnable() {
          @Override
          public void run() {
            delayedRuns.incrementAndGet();
            subExecutor.scheduleAtFixedRate(new Runnable() {
              @Override
              public void run() {
                periodicRuns.incrementAndGet();
              }
            }, subDelay, subPeriod, TimeUnit.MILLISECONDS);
          }
        }, delay, TimeUnit.MILLISECONDS);
        ++expectedDelayedRuns;
        expectedPeriodicRuns += 1 + (TIME - delay - subDelay) / subPeriod;
      }
      break;

      }

    Thread.sleep(TIME);

    final Set<Future<Boolean>> futures = new HashSet<Future<Boolean>>();
    for (int i = 0; i < CORES; ++i)
      try {
        futures.add(subExecutor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            subExecutor.shutdown();
            return true;
          }
        }));
      } catch (RejectedExecutionException e) {
        // it's ok here
      }

    subExecutor.shutdown();
    assertTrue(subExecutor.awaitTermination(TIME, TimeUnit.MILLISECONDS));
    assertTrue(subExecutor.isTerminated());

    for (Future<Boolean> future : futures)
      assertTrue(future.get());

    final long runsSnapshot = runs.get();
    final long delayedRunsSnapshot = delayedRuns.get();
    final long periodicRunsSnapshot = periodicRuns.get();

    assertEquals(expectedRuns, runsSnapshot);
    assertEquals(expectedDelayedRuns, delayedRunsSnapshot);
    assertTrue(fuzzyGreater(periodicRunsSnapshot, expectedPeriodicRuns));
    assertTrue(fuzzyLess(periodicRunsSnapshot, expectedPeriodicRuns * 2));

    Thread.sleep(TIME / 5);
    assertEquals(runsSnapshot, runs.get());
    assertEquals(delayedRunsSnapshot, delayedRuns.get());
    assertEquals(periodicRunsSnapshot, periodicRuns.get());
  }

  private static boolean fuzzyGreater(long actual, long expected) {
    return actual > expected - (expected / 4.0); // 25% tolerance
  }

  private static boolean fuzzyLess(long actual, long expected) {
    return actual < expected + (expected / 4.0); // 25% tolerance
  }

}
