package com.orientechnologies.common.thread;

import com.orientechnologies.common.log.OLogManager;
import java.util.concurrent.*;

/**
 * The same as thread {@link ScheduledThreadPoolExecutor} but also logs all exceptions happened
 * inside of the tasks which caused tasks to stop.
 */
public class OScheduledThreadPoolExecutorWithLogging extends ScheduledThreadPoolExecutor
    implements TracingScheduledExecutorService {
  public OScheduledThreadPoolExecutorWithLogging(int corePoolSize) {
    super(corePoolSize);
  }

  public OScheduledThreadPoolExecutorWithLogging(int corePoolSize, ThreadFactory threadFactory) {
    super(corePoolSize, threadFactory);
  }

  @SuppressWarnings("unused")
  public OScheduledThreadPoolExecutorWithLogging(
      int corePoolSize, RejectedExecutionHandler handler) {
    super(corePoolSize, handler);
  }

  @SuppressWarnings("unused")
  public OScheduledThreadPoolExecutorWithLogging(
      int corePoolSize, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
    super(corePoolSize, threadFactory, handler);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);

    if ((t == null) && r instanceof Future<?>) {
      final Future<?> future = (Future<?>) r;
      // scheduled futures can block execution forever if they are not done
      if (future.isDone()) {
        try {
          future.get();
        } catch (CancellationException ce) {
          // ignore it we cancel tasks on shutdown that is normal
        } catch (ExecutionException ee) {
          t = ee.getCause();
        } catch (InterruptedException ie) {
          Thread.currentThread().interrupt(); // ignore/reset
        }
      }
    }

    if (t != null) {
      final Thread thread = Thread.currentThread();
      OLogManager.instance().errorNoDb(this, "Exception in thread '%s'", t, thread.getName());
    }
  }

  @Override
  public <T> Future<T> submit(String taskName, Callable<T> task) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(taskName, task);
    return super.submit(
        () -> {
          try {
            return task.call();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, task);
          }
        });
  }

  @Override
  public Future<?> submit(String taskName, Runnable task) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(taskName, task);
    return super.submit(
        () -> {
          try {
            task.run();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, task);
          }
        });
  }

  @Override
  public <T> Future<T> submit(String taskName, Runnable task, T result) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(taskName, task);
    return super.submit(
        () -> {
          try {
            task.run();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, task);
          }
        },
        result);
  }

  @Override
  public void execute(String taskName, Runnable command) {
    final OTracedExecutionException trace =
        OTracedExecutionException.prepareTrace(taskName, command);
    super.execute(
        () -> {
          try {
            command.run();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, command);
          }
        });
  }

  @Override
  public Future<?> submit(Runnable task) {
    return submit((String) null, task);
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    return submit((String) null, task);
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    return submit(null, task, result);
  }

  @Override
  public void execute(Runnable command) {
    execute(null, command);
  }

  @Override
  public ScheduledFuture<?> schedule(String taskName, Runnable command, long delay, TimeUnit unit) {
    final OTracedExecutionException trace =
        OTracedExecutionException.prepareTrace(taskName, command);
    return super.schedule(
        () -> {
          try {
            command.run();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, command);
          }
        },
        delay,
        unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(
      String taskName, Callable<V> task, long delay, TimeUnit unit) {
    final OTracedExecutionException trace = OTracedExecutionException.prepareTrace(taskName, task);
    return super.schedule(
        () -> {
          try {
            return task.call();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, task);
          }
        },
        delay,
        unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      String taskName, Runnable command, long initialDelay, long period, TimeUnit unit) {
    final OTracedExecutionException trace =
        OTracedExecutionException.prepareTrace(taskName, command);
    return super.scheduleAtFixedRate(
        () -> {
          try {
            command.run();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, command);
          }
        },
        initialDelay,
        period,
        unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      String taskName, Runnable command, long initialDelay, long delay, TimeUnit unit) {
    final OTracedExecutionException trace =
        OTracedExecutionException.prepareTrace(taskName, command);
    return super.scheduleWithFixedDelay(
        () -> {
          try {
            command.run();
          } catch (Exception e) {
            throw OTracedExecutionException.trace(trace, e, taskName, command);
          }
        },
        initialDelay,
        delay,
        unit);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    return schedule(null, command, delay, unit);
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    return schedule(null, callable, delay, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(
      Runnable command, long initialDelay, long period, TimeUnit unit) {
    return scheduleAtFixedRate(null, command, initialDelay, period, unit);
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(
      Runnable command, long initialDelay, long delay, TimeUnit unit) {
    return scheduleWithFixedDelay(null, command, initialDelay, delay, unit);
  }
}
