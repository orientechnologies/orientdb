package com.orientechnologies.common.thread;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class OSourceTraceExecutorService implements ExecutorService {

  private final ExecutorService service;

  public OSourceTraceExecutorService(ExecutorService service) {
    Objects.requireNonNull(service);
    this.service = service;
  }

  @Override
  public void execute(Runnable command) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(command);
    this.service.execute(
        () -> {
          try {
            command.run();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, command);
          }
        });
  }

  @Override
  public void shutdown() {
    this.service.shutdown();
  }

  @Override
  public List<Runnable> shutdownNow() {
    return this.service.shutdownNow();
  }

  @Override
  public boolean isShutdown() {
    return this.service.isShutdown();
  }

  @Override
  public boolean isTerminated() {
    return this.service.isTerminated();
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    return this.service.awaitTermination(timeout, unit);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(task);
    return this.service.submit(
        () -> {
          try {
            return task.call();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, task);
          }
        });
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(task);
    return this.service.submit(
        () -> {
          try {
            task.run();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, task);
          }
        },
        result);
  }

  @Override
  public Future<?> submit(Runnable task) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(task);
    return this.service.submit(
        () -> {
          try {
            task.run();
          } catch (RuntimeException e) {
            throw OTracedExecutionException.trace(trace, e, task);
          }
        });
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    return this.service.invokeAll(tasks);
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    return this.service.invokeAll(tasks, timeout, unit);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    return this.service.invokeAny(tasks);
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return this.service.invokeAny(tasks, timeout, unit);
  }
}
