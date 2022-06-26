package com.orientechnologies.common.thread;

import java.util.concurrent.*;

public class OThreadPoolExecutors {

  private OThreadPoolExecutors() {}

  public static ExecutorService newScalingThreadPool(
      String threadName,
      int corePoolSize,
      int maxPoolSize,
      int queueCapacity,
      long timeout,
      TimeUnit timeoutUnit) {
    return newScalingThreadPool(
        threadName,
        Thread.currentThread().getThreadGroup(),
        corePoolSize,
        maxPoolSize,
        queueCapacity,
        timeout,
        timeoutUnit);
  }

  public static ExecutorService newScalingThreadPool(
      String threadName,
      ThreadGroup parentThreadGroup,
      int corePoolSize,
      int maxPoolSize,
      int queueCapacity,
      long timeout,
      TimeUnit timeoutUnit) {
    return new ScalingThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        timeout,
        timeoutUnit,
        queueCapacity,
        new NamedThreadFactory(threadName, parentThreadGroup));
  }

  public static ExecutorService newBlockingScalingThreadPool(
      String threadName,
      ThreadGroup parentThreadGroup,
      int corePoolSize,
      int maxPoolSize,
      int targetQueueCapacity,
      int maxQueueCapacity,
      long timeout,
      TimeUnit timeoutUnit) {
    return new ScalingThreadPoolExecutor(
        corePoolSize,
        maxPoolSize,
        timeout,
        timeoutUnit,
        targetQueueCapacity,
        maxQueueCapacity,
        new NamedThreadFactory(threadName, parentThreadGroup));
  }

  public static ExecutorService newFixedThreadPool(String threadName, int poolSize) {
    return newFixedThreadPool(threadName, Thread.currentThread().getThreadGroup(), poolSize);
  }

  public static ExecutorService newFixedThreadPool(
      String threadName, ThreadGroup parentThreadGroup, int poolSize) {
    return new OThreadPoolExecutorWithLogging(
        poolSize,
        poolSize,
        0,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new NamedThreadFactory(threadName, parentThreadGroup));
  }

  public static ExecutorService newCachedThreadPool(String threadName) {
    return newCachedThreadPool(threadName, Thread.currentThread().getThreadGroup());
  }

  public static ExecutorService newCachedThreadPool(
      String threadName, ThreadGroup parentThreadGroup) {
    return newCachedThreadPool(threadName, parentThreadGroup, Integer.MAX_VALUE, 0);
  }

  public static ExecutorService newCachedThreadPool(
      String threadName, ThreadGroup parentThreadGroup, int maxThreads, int maxQueue) {
    return new OThreadPoolExecutorWithLogging(
        0,
        maxThreads,
        60L,
        TimeUnit.SECONDS,
        maxQueue <= 0 ? new SynchronousQueue<>() : new LinkedBlockingQueue<>(maxQueue),
        new NamedThreadFactory(threadName, parentThreadGroup));
  }

  public static ExecutorService newSingleThreadPool(String threadName) {
    return newSingleThreadPool(threadName, Thread.currentThread().getThreadGroup());
  }

  public static ExecutorService newSingleThreadPool(
      String threadName, ThreadGroup parentThreadGroup) {
    return new OThreadPoolExecutorWithLogging(
        1,
        1,
        0,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(),
        new SingletonNamedThreadFactory(threadName, parentThreadGroup));
  }

  public static ExecutorService newSingleThreadPool(
      String threadName, int maxQueue, RejectedExecutionHandler rejectHandler) {
    return new OThreadPoolExecutorWithLogging(
        1,
        1,
        0,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(maxQueue),
        new SingletonNamedThreadFactory(threadName, Thread.currentThread().getThreadGroup()),
        rejectHandler);
  }

  public static ScheduledExecutorService newSingleThreadScheduledPool(String threadName) {
    return newSingleThreadScheduledPool(threadName, Thread.currentThread().getThreadGroup());
  }

  public static ScheduledExecutorService newSingleThreadScheduledPool(
      String threadName, ThreadGroup parentThreadGroup) {
    return new OScheduledThreadPoolExecutorWithLogging(
        1, new SingletonNamedThreadFactory(threadName, parentThreadGroup));
  }
}
