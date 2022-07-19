package com.orientechnologies.common.thread;

import com.orientechnologies.common.log.OLogManager;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionHandler;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The same as thread {@link ThreadPoolExecutor} but also logs all exceptions happened inside of the
 * tasks which caused tasks to stop.
 */
public class OThreadPoolExecutorWithLogging extends ThreadPoolExecutor
    implements TracingExecutorService {
  public OThreadPoolExecutorWithLogging(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  public OThreadPoolExecutorWithLogging(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  @SuppressWarnings("unused")
  public OThreadPoolExecutorWithLogging(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
  }

  @SuppressWarnings("unused")
  public OThreadPoolExecutorWithLogging(
      int corePoolSize,
      int maximumPoolSize,
      long keepAliveTime,
      TimeUnit unit,
      BlockingQueue<Runnable> workQueue,
      ThreadFactory threadFactory,
      RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);

    if ((t == null) && (r instanceof Future<?>)) {
      final Future<?> future = (Future<?>) r;
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
}
