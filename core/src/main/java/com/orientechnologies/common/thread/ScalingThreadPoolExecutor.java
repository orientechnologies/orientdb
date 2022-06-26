package com.orientechnologies.common.thread;

import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * A {@link ThreadPoolExecutor} that scales the pool beyond the core pool size, even if an unbounded
 * work queue is used.
 */
class ScalingThreadPoolExecutor extends OThreadPoolExecutorWithLogging {

  /**
   * An optionally bounded work queue for a {@link ThreadPoolExecutor} that causes the pool to scale
   * beyond the {@link ThreadPoolExecutor#getCorePoolSize() core pool size}. <br>
   * This is achieved by defining a target queue capacity, and wiring the {@link
   * RejectedExecutionHandler} for the thread pool to re-queue using {@link #safeOffer(Runnable)}.
   * <br>
   * The work queue will probabilistically reject {@link #offer(Runnable) offers} to the work queue
   * (with that probability growing to 100% as the indicated capacity is reached). The rejection of
   * the offer will trigger pool growth in the thread pool, and once the max pool size has been
   * reached will cause {@link RejectedExecutionHandler#rejectedExecution(Runnable,
   * ThreadPoolExecutor) rejection}. <br>
   * The implementation of {@link RejectedExecutionHandler} for the executor will then add it
   * directly to the work queue, bypassing the rejection logic.
   */
  private static class ScalingQueue extends LinkedBlockingQueue<Runnable> {
    private final int targetCapacity;

    private final Random rand = new Random();

    private volatile boolean maxPoolReached = false;

    public ScalingQueue(int targetCapacity) {
      super(); // Don't limit actual queue - capacity is used to signal ThreadPoolExecutor to grow
      this.targetCapacity = targetCapacity;
    }

    public ScalingQueue(int targetCapacity, int maxQueueCapacity) {
      super(maxQueueCapacity);
      if (targetCapacity >= maxQueueCapacity) {
        throw new IllegalArgumentException("Target capacity must be less than max queue capacity");
      }
      this.targetCapacity = targetCapacity;
    }

    @Override
    public boolean offer(Runnable r) {
      if (isEmpty()) {
        maxPoolReached = false;
      }
      if (!maxPoolReached) {
        final int size = size();
        final int trigger =
            targetCapacity <= 1 ? (targetCapacity - 1) : rand.nextInt(targetCapacity);
        if (size > trigger) {
          return false;
        }
      }
      return super.offer(r);
    }

    protected void safeOffer(Runnable r) {
      // On unbounded/target queue, will always succeed immediately. For bounded, may block.
      try {
        super.put(r);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RejectedExecutionException(e);
      }
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

  public ScalingThreadPoolExecutor(
      int corePoolSize,
      int maxPoolSize,
      long timeout,
      TimeUnit timeoutUnit,
      int targetQueueCapacity,
      int maxQueueCapacity,
      NamedThreadFactory namedThreadFactory) {
    super(
        corePoolSize,
        maxPoolSize,
        timeout,
        timeoutUnit,
        new ScalingQueue(targetQueueCapacity, maxQueueCapacity),
        namedThreadFactory,
        (r, executor) -> ((ScalingQueue) executor.getQueue()).safeOffer(r));
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);
    ((ScalingQueue) getQueue()).setMaxPoolReached(getPoolSize() == getMaximumPoolSize());
  }
}
