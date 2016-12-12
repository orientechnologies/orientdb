/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.common.concur.executors;

import java.util.ArrayList;
import java.util.Set;
import java.util.concurrent.*;

/**
 * Scheduled version of {@link SubExecutorService}. Supports delegation to {@link ScheduledThreadPoolExecutor} only.
 *
 * @author Sergey Sitnikov
 */
@SuppressWarnings({ "unchecked", "NullableProblems" })
public class SubScheduledExecutorService extends SubExecutorService implements ScheduledExecutorService {

  /**
   * Constructs a new SubExecutorService for the given executor service.
   *
   * @param executorService the underlying executor service to submit tasks to
   */
  public SubScheduledExecutorService(ScheduledThreadPoolExecutor executorService) {
    super(executorService);
  }

  @Override
  public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
    acquireAlive();
    try {
      if (isAlive()) {
        final ScheduledTask wrapped = new ScheduledRunnableTask(command, false);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().schedule((Runnable) wrapped, delay, unit));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else
        return throwRejected(command);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
    acquireAlive();
    try {
      if (isAlive()) {
        final ScheduledTask wrapped = new ScheduledCallableTask(callable, false);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().schedule((Callable) wrapped, delay, unit));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else
        return throwRejected(callable);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public ScheduledFuture<?> scheduleAtFixedRate(Runnable command, long initialDelay, long period, TimeUnit unit) {
    acquireAlive();
    try {
      if (isAlive()) {
        final ScheduledTask wrapped = new ScheduledRunnableTask(command, true);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().scheduleAtFixedRate(wrapped, initialDelay, period, unit));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else
        return throwRejected(command);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public ScheduledFuture<?> scheduleWithFixedDelay(Runnable command, long initialDelay, long delay, TimeUnit unit) {
    acquireAlive();
    try {
      if (isAlive()) {
        final ScheduledTask wrapped = new ScheduledRunnableTask(command, true);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().scheduleWithFixedDelay(wrapped, initialDelay, delay, unit));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else
        return throwRejected(command);
    } finally {
      releaseAlive();
    }
  }

  @Override
  protected ScheduledThreadPoolExecutor getExecutorService() {
    return (ScheduledThreadPoolExecutor) super.getExecutorService();
  }

  @Override
  protected void shutdownTasks(Set<Task> tasks) {
    final ScheduledThreadPoolExecutor executorService = getExecutorService();
    final BlockingQueue<Runnable> queue = executorService.getQueue();
    final boolean abortPeriodic = !executorService.getContinueExistingPeriodicTasksAfterShutdownPolicy();
    final boolean abortDelayed = !executorService.getExecuteExistingDelayedTasksAfterShutdownPolicy();

    for (Task task : new ArrayList<Task>(tasks))
      if (task instanceof ScheduledTask) {
        final ScheduledTask scheduledTask = (ScheduledTask) task;
        final boolean cancelled = task.isCancelled();

        if (scheduledTask.isPeriodic()) {
          if (abortPeriodic || cancelled) {
            task.acquireExecution();
            try {
              //noinspection SuspiciousMethodCalls
              if (queue.remove(task.getFuture()))
                try {
                  if (!cancelled)
                    task.cancel(false);
                } finally {
                  unregister(task);
                }
              else {
                if (!cancelled)
                  task.cancel(false);
                unregister(task); // no try/finally, if the cancelation is failed an active task may be lost from the registry
              }
            } finally {
              task.releaseExecution();
            }
          }
        } else if (abortDelayed || cancelled) {
          //noinspection SuspiciousMethodCalls
          if (queue.remove(task.getFuture()))
            try {
              if (!cancelled)
                task.cancel(false);
            } finally {
              unregister(task);
            }
        }
      }

    super.shutdownTasks(tasks);
  }

  protected interface ScheduledTask<V> extends Task<V>, ScheduledFuture<V> {
    @Override
    ScheduledFuture<V> getFuture();

    void setFuture(ScheduledFuture<V> future);

    boolean isPeriodic();
  }

  protected class ScheduledRunnableTask<V> extends RunnableTask<V> implements ScheduledTask<V> {

    private final boolean periodic;

    public ScheduledRunnableTask(Runnable runnable, boolean periodic) {
      super(runnable, !periodic);
      this.periodic = periodic;
    }

    @Override
    public ScheduledFuture<V> getFuture() {
      return (ScheduledFuture) super.getFuture();
    }

    @Override
    public void setFuture(ScheduledFuture<V> future) {
      super.setFuture(future);
    }

    @Override
    public boolean isPeriodic() {
      return periodic;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return getFuture().getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
      return getFuture().compareTo(o);
    }

  }

  protected class ScheduledCallableTask<V> extends CallableTask<V> implements ScheduledTask<V> {

    private final boolean periodic;

    public ScheduledCallableTask(Callable<V> callable, boolean periodic) {
      super(callable, !periodic);
      this.periodic = periodic;
    }

    @Override
    public ScheduledFuture<V> getFuture() {
      return (ScheduledFuture) super.getFuture();
    }

    @Override
    public void setFuture(ScheduledFuture<V> future) {
      super.setFuture(future);
    }

    @Override
    public boolean isPeriodic() {
      return periodic;
    }

    @Override
    public long getDelay(TimeUnit unit) {
      return getFuture().getDelay(unit);
    }

    @Override
    public int compareTo(Delayed o) {
      return getFuture().compareTo(o);
    }

  }

}
