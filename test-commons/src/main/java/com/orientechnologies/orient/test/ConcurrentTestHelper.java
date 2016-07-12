package com.orientechnologies.orient.test;

import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * @author Artem Orobets (enisher-at-gmail.com)
 * @param <T> see {@link TestFactory}
 */
public class ConcurrentTestHelper<T> {
  private final ExecutorService executor;
  private final List<Future<T>> futures;

  public static <T> Collection<T> test(int threadCount, TestFactory<T> factory) {
    final List<Callable<T>> callables = prepareWorkers(threadCount, factory);
    return go(callables);
  }

  protected static <T> Collection<T> go(List<Callable<T>> workers) {
    final ConcurrentTestHelper<T> helper = new ConcurrentTestHelper<T>(workers.size());

    helper.submit(workers);

    return helper.assertSuccess();
  }

  protected static <T> List<Callable<T>> prepareWorkers(int threadCount, TestFactory<T> factory) {
    final List<Callable<T>> callables = new ArrayList<Callable<T>>(threadCount);
    for (int i = 0; i < threadCount; i++) {
      callables.add(factory.createWorker());
    }
    return callables;
  }

  public static <T> TestBuilder<T> build() {
    return new TestBuilder<T>();
  }

  private Collection<T> assertSuccess() {
    try {
      executor.shutdown();
      assertTrue(executor.awaitTermination(30, TimeUnit.MINUTES), "Test threads hanged");

      List<T> results = new ArrayList<T>(futures.size());
      List<Exception> exceptions = new ArrayList<Exception>();
      for (Future<T> future : futures) {
        try {
          results.add(future.get());
        } catch (ExecutionException e) {
          exceptions.add(e);
        }
      }

      if (exceptions.isEmpty()) {
        return results;
      } else {
        throw new CompositeException(exceptions);
      }
    } catch (InterruptedException e) {
      fail("interrupted", e);
      throw new RuntimeException("unreached exception", e);
    }
  }

  private void submit(List<Callable<T>> callables) {
    for (Callable<T> callable : callables) {
      futures.add(executor.submit(callable));
    }
  }

  private ConcurrentTestHelper(int threadCount) {
    this.futures = new ArrayList<Future<T>>(threadCount);
    this.executor = Executors.newFixedThreadPool(threadCount);
  }

}
