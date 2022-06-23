package com.orientechnologies.common.thread;

import java.util.Random;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class OThreadPoolExecutors {

  private OThreadPoolExecutors() {}

  /**
   * A {@link ThreadPoolExecutor} with an unbounded work queue, that still scales the pool beyond
   * the core pool size.
   */
  protected static class ScalingThreadPoolExecutor extends OThreadPoolExecutorWithLogging {

    /**
     * An unbounded work queue for a {@link ThreadPoolExecutor} that causes the pool to scale beyond
     * the {@link ThreadPoolExecutor#getCorePoolSize()} core pool size}. <br>
     * This is achieved by defining a target queue capacity, and wiring the {@link
     * RejectedExecutionHandler} for the thread pool to re-queue using {@link #safeOffer(Runnable)}.
     * <br>
     * The work queue will probabilistically reject {@link #offer(Runnable) offers} to the work
     * queue (with that probability growing to 100% as the indicated capacity is reached). The
     * rejection of the offer will trigger pool growth in the thread pool, and once the max pool
     * size has been reached will cause {@link RejectedExecutionHandler#rejectedExecution(Runnable,
     * ThreadPoolExecutor) rejection}. <br>
     * The implementation of {@link RejectedExecutionHandler} for this queue will then add it
     * directly to the work queue, bypassing the rejection logic.
     */
    private static class ScalingQueue extends LinkedBlockingQueue<Runnable> {
      private final int capacity;

      private final Random rand = new Random();

      private volatile boolean maxPoolReached = false;

      public ScalingQueue(int capacity) {
        super(); // Don't limit actual queue - capacity is used to signal ThreadPoolExecutor to grow
        this.capacity = capacity;
      }

      @Override
      public boolean offer(Runnable r) {
        if (isEmpty()) {
          maxPoolReached = false;
        }
        if (!maxPoolReached) {
          final int size = size();
          final int trigger = capacity <= 1 ? (capacity - 1) : rand.nextInt(capacity);
          if (size > trigger) {
            return false;
          }
        }
        return super.offer(r);
      }

      protected void safeOffer(Runnable r) {
        super.offer(r); // Queue is unbounded, so offer will succceed
      }

      protected void setMaxPoolReached(boolean maxPoolReached) {
        this.maxPoolReached = maxPoolReached;
      }
    }

    public ScalingThreadPoolExecutor(
        int corePoolSize,
        int maxPoolSize,
        long timeout,
        TimeUnit timeoutUnit,
        int queueCapacity,
        NamedThreadFactory namedThreadFactory) {
      super(
          corePoolSize,
          maxPoolSize,
          timeout,
          timeoutUnit,
          new ScalingQueue(queueCapacity),
          namedThreadFactory,
          (r, executor) -> ((ScalingQueue) executor.getQueue()).safeOffer(r));
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
      super.afterExecute(r, t);
      ((ScalingQueue) getQueue()).setMaxPoolReached(getPoolSize() == getMaximumPoolSize());
    }
  }

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

  private abstract static class BaseThreadFactory implements ThreadFactory {

    private final ThreadGroup parentThreadGroup;

    protected BaseThreadFactory(ThreadGroup parentThreadGroup) {
      this.parentThreadGroup = parentThreadGroup;
    }

    @Override
    public final Thread newThread(final Runnable r) {
      final Thread thread = new Thread(parentThreadGroup, r);
      thread.setDaemon(true);
      thread.setName(nextThreadName());
      return thread;
    }

    protected abstract String nextThreadName();
  }

  private static final class SingletonNamedThreadFactory extends BaseThreadFactory {

    private final String name;

    private SingletonNamedThreadFactory(final String name, ThreadGroup parentThreadGroup) {
      super(parentThreadGroup);
      this.name = name;
    }

    @Override
    protected String nextThreadName() {
      return name;
    }
  }

  private static final class NamedThreadFactory extends BaseThreadFactory {

    private final String baseName;

    private final AtomicInteger threadId = new AtomicInteger(1);

    private NamedThreadFactory(final String baseName, ThreadGroup parentThreadGroup) {
      super(parentThreadGroup);
      this.baseName = baseName;
    }

    @Override
    protected String nextThreadName() {
      return String.format("%s-%d", baseName, threadId.getAndIncrement());
    }
  }
}
