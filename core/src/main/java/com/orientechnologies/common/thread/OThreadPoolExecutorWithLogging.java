package com.orientechnologies.common.thread;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OSQLDumper;
import com.orientechnologies.orient.core.Orient;

import java.io.StringWriter;
import java.util.Collection;
import java.util.concurrent.*;

/**
 * The same as thread {@link ThreadPoolExecutor} but also logs all exceptions happened inside of the tasks which caused tasks to
 * stop.
 */
public class OThreadPoolExecutorWithLogging extends ThreadPoolExecutor {
  public OThreadPoolExecutorWithLogging(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue);
  }

  public OThreadPoolExecutorWithLogging(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
  }

  public OThreadPoolExecutorWithLogging(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue, RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, handler);
  }

  public OThreadPoolExecutorWithLogging(int corePoolSize, int maximumPoolSize, long keepAliveTime, TimeUnit unit,
      BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory, RejectedExecutionHandler handler) {
    super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory, handler);
  }

  @Override
  protected void afterExecute(Runnable r, Throwable t) {
    super.afterExecute(r, t);

    if (r instanceof Future<?>) {
      final Future<?> future = (Future<?>) r;
      try {
        future.get();
      } catch (CancellationException ce) {
        //ignore it we cancel tasks on shutdown that is normal
      } catch (ExecutionException ee) {
        t = ee.getCause();
      } catch (InterruptedException ie) {
        Thread.currentThread().interrupt(); // ignore/reset
      }
    }

    if (t != null) {
      final Thread thread = Thread.currentThread();
      final OLogManager logManager = OLogManager.instance();

      if (logManager != null) {
        if (t instanceof OutOfMemoryError) {
          final Collection<String> queries = OSQLDumper.dumpAllSQLQueries();
          if (queries.isEmpty())
            logManager.errorNoDb(this, "Uncaught exception in thread '%s'", t, thread.getName());
          else {
            final StringBuilder sb = new StringBuilder();
            sb.append("OOM Error was thrown by JVM. OOM can be caused by one of the following queries: \n");
            sb.append("-----------------------------------------------------------------------------------\n");
            for (String query : queries) {
              sb.append("- '").append(query).append("'\n");
            }
            sb.append("-----------------------------------------------------------------------------------\n");

            logManager.errorNoDb(this, sb.toString(), t);
          }
        } else {
          logManager.errorNoDb(this, "Uncaught exception in thread '%s'", t, thread.getName());
        }
      }
    }
  }
}
