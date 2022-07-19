package com.orientechnologies.common.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public interface TracingExecutorService extends ExecutorService {

  <T> Future<T> submit(String taskName, Callable<T> task);

  Future<?> submit(String taskName, Runnable task);

  <T> Future<T> submit(String taskName, Runnable task, T result);

  void execute(String taskName, Runnable command);
}
