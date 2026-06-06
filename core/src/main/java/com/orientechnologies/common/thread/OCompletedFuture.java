package com.orientechnologies.common.thread;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public final class OCompletedFuture<T> implements Future<T> {
  private final T value;

  public OCompletedFuture(T value) {
    this.value = value;
  }

  @Override
  public boolean isDone() {
    return true;
  }

  @Override
  public boolean isCancelled() {
    return false;
  }

  @Override
  public T get(long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    return value;
  }

  @Override
  public T get() throws InterruptedException, ExecutionException {
    return value;
  }

  @Override
  public boolean cancel(boolean mayInterruptIfRunning) {
    return false;
  }
}
