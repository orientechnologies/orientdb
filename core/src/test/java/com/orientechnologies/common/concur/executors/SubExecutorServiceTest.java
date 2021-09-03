/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class SubExecutorServiceTest {

  private ExecutorService executor;
  private ExecutorService subExecutor;

  @Before
  public void before() {
    executor = Executors.newSingleThreadExecutor();
    subExecutor = new SubExecutorService(executor);
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
  public void testSubmitCallable() throws ExecutionException, InterruptedException {
    final AtomicBoolean ran = new AtomicBoolean(false);

    final Boolean result =
        subExecutor
            .submit(
                new Callable<Boolean>() {
                  @Override
                  public Boolean call() throws Exception {
                    ran.set(true);
                    return true;
                  }
                })
            .get();

    assertTrue(result);
    assertTrue(ran.get());
  }

  @Test
  public void testSubmitRunnableWithResult() throws ExecutionException, InterruptedException {
    final AtomicBoolean ran = new AtomicBoolean(false);

    final Boolean result =
        subExecutor
            .submit(
                new Runnable() {
                  @Override
                  public void run() {
                    ran.set(true);
                  }
                },
                true)
            .get();

    assertTrue(result);
    assertTrue(ran.get());
  }

  @Test
  public void testSubmitRunnable() throws ExecutionException, InterruptedException {
    final AtomicBoolean ran = new AtomicBoolean(false);

    final Object result =
        subExecutor
            .submit(
                new Runnable() {
                  @Override
                  public void run() {
                    ran.set(true);
                  }
                })
            .get();

    assertNull(result);
    assertTrue(ran.get());
  }

  @Test
  public void testExecute() throws Exception {
    final AtomicBoolean ran = new AtomicBoolean(false);

    subExecutor.execute(
        new Runnable() {
          @Override
          public void run() {
            ran.set(true);
          }
        });

    assertTrue(
        busyWait(
            new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                return ran.get();
              }
            }));
  }

  @Test(expected = CancellationException.class)
  public void testCancelNotDone() throws Exception {
    final AtomicBoolean started = new AtomicBoolean(false);
    final AtomicBoolean stop = new AtomicBoolean(false);

    final Future<Boolean> future =
        subExecutor.submit(
            new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                started.set(true);

                assertTrue(
                    busyWait(
                        new Callable<Boolean>() {
                          @Override
                          public Boolean call() throws Exception {
                            return stop.get();
                          }
                        }));

                return true;
              }
            });

    assertTrue(
        busyWait(
            new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                return started.get();
              }
            }));

    assertFalse(future.isCancelled());
    assertFalse(future.isDone());
    assertTrue(future.cancel(false));
    assertTrue(future.isCancelled());
    assertTrue(future.isDone());

    stop.set(true);
    future.get(); // should throw CancellationException
  }

  @Test
  public void testShutdown() throws Exception {
    final AtomicBoolean shutdown = new AtomicBoolean(false);

    subExecutor.submit(
        new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            assertTrue(
                busyWait(
                    new Callable<Boolean>() {
                      @Override
                      public Boolean call() throws Exception {
                        return shutdown.get();
                      }
                    }));
            return true;
          }
        });

    subExecutor.shutdown();
    assertTrue(subExecutor.isShutdown());
    assertFalse(subExecutor.isTerminated());
    shutdown.set(true);

    assertTrue(
        busyWait(
            new Callable<Boolean>() {
              @Override
              public Boolean call() throws Exception {
                return subExecutor.isTerminated();
              }
            }));

    assertTrue(subExecutor.isShutdown());
    assertTrue(subExecutor.isTerminated());
    assertFalse(executor.isShutdown());
    assertFalse(executor.isTerminated());
  }

  @Test(expected = RejectedExecutionException.class)
  public void testRejected() {
    subExecutor.shutdown();

    subExecutor.submit(
        new Runnable() {
          @Override
          public void run() {}
        });
  }

  @Test
  public void testTaskFailure() throws InterruptedException {
    boolean thrown;
    try {
      subExecutor
          .submit(
              new Runnable() {
                @Override
                public void run() {
                  throw new TaskFailureException();
                }
              })
          .get();
      thrown = false;
    } catch (ExecutionException e) {
      assertTrue(e.getCause() instanceof TaskFailureException);
      thrown = true;
    }

    assertTrue(thrown);
  }

  private static boolean busyWait(Callable<Boolean> condition) throws Exception {
    if (condition.call()) return true;

    final long start = System.currentTimeMillis();
    do {
      Thread.sleep(20);

      if (condition.call()) return true;

      if (System.currentTimeMillis() - start >= 5000) return false;
    } while (true);
  }

  private static class TaskFailureException extends RuntimeException {}
}
