package com.orientechnologies.common.thread;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public interface TracingScheduledExecutorService
    extends TracingExecutorService, ScheduledExecutorService {

  ScheduledFuture<?> schedule(String taskName, Runnable command, long delay, TimeUnit unit);

  <V> ScheduledFuture<V> schedule(String taskName, Callable<V> callable, long delay, TimeUnit unit);

  ScheduledFuture<?> scheduleAtFixedRate(
      String taskName, Runnable command, long initialDelay, long period, TimeUnit unit);

  ScheduledFuture<?> scheduleWithFixedDelay(
      String taskName, Runnable command, long initialDelay, long delay, TimeUnit unit);
}
