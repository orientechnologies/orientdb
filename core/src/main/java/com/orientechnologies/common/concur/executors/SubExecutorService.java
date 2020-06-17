/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Limits the tasks scope of an {@link ExecutorService} into a smaller sub-executor service. This
 * allows to submit tasks to the underlying executor service while the shutdown related methods are
 * scoped only to the subset of tasks submitted through this sub-executor:
 *
 * <ul>
 *   <li>{@link #shutdown()} – shutdowns this sub-executor only.
 *   <li>{@link #isShutdown()} and {@link #isTerminated()} – report status of this sub-executor
 *       only.
 *   <li>{@link #awaitTermination(long, TimeUnit)} – awaits for tasks submitted through this
 *       sub-executor only.
 * </ul>
 *
 * @author Sergey Sitnikov
 */
@SuppressWarnings({"unchecked", "NullableProblems"})
public class SubExecutorService implements ExecutorService {

  private final ExecutorService executorService;

  private boolean alive = true;

  private final Lock aliveLock = new ReentrantLock();
  private final Condition terminated = aliveLock.newCondition();

  private final Set<Task> tasks = new HashSet<Task>();

  /**
   * Constructs a new SubExecutorService for the given executor service.
   *
   * @param executorService the underlying executor service to submit tasks to
   */
  public SubExecutorService(ExecutorService executorService) {
    this.executorService = executorService;
  }

  @Override
  public void shutdown() {
    acquireAlive();
    try {
      alive = false;
      shutdownTasks(tasks);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public List<Runnable> shutdownNow() {
    throw new UnsupportedOperationException("shutdownNow is not supported");
  }

  @Override
  public boolean isShutdown() {
    acquireAlive();
    try {
      return !isAlive();
    } finally {
      releaseAlive();
    }
  }

  @Override
  public boolean isTerminated() {
    acquireAlive();
    try {
      return !isRunning();
    } finally {
      releaseAlive();
    }
  }

  @Override
  public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
    acquireAlive();
    try {
      return !isRunning() || terminated.await(timeout, unit);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public <T> Future<T> submit(Callable<T> task) {
    acquireAlive();
    try {
      if (isAlive()) {
        final Task wrapped = new CallableTask(task, true);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().submit((Callable) wrapped));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else return throwRejected(task);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public <T> Future<T> submit(Runnable task, T result) {
    acquireAlive();
    try {
      if (isAlive()) {
        final Task wrapped = new RunnableTask(task, true);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().submit(wrapped, result));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else return throwRejected(task);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public Future<?> submit(Runnable task) {
    acquireAlive();
    try {
      if (isAlive()) {
        final Task wrapped = new RunnableTask(task, true);
        wrapped.acquireExecution();
        try {
          wrapped.setFuture(getExecutorService().submit((Runnable) wrapped));
          return register(wrapped);
        } finally {
          wrapped.releaseExecution();
        }
      } else return throwRejected(task);
    } finally {
      releaseAlive();
    }
  }

  @Override
  public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks)
      throws InterruptedException {
    throw new UnsupportedOperationException("invokeAll is not supported");
  }

  @Override
  public <T> List<Future<T>> invokeAll(
      Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException {
    throw new UnsupportedOperationException("invokeAll is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks)
      throws InterruptedException, ExecutionException {
    throw new UnsupportedOperationException("invokeAny is not supported");
  }

  @Override
  public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit)
      throws InterruptedException, ExecutionException, TimeoutException {
    throw new UnsupportedOperationException("invokeAny is not supported");
  }

  @Override
  public void execute(Runnable command) {
    submit(command);
  }

  @Override
  public String toString() {
    return "Sub(" + getExecutorService().toString() + ")";
  }

  protected void acquireAlive() {
    aliveLock.lock();
  }

  protected void releaseAlive() {
    aliveLock.unlock();
  }

  protected boolean isAlive() {
    return alive;
  }

  protected boolean isRunning() {
    return isAlive() || !tasks.isEmpty();
  }

  protected ExecutorService getExecutorService() {
    return executorService;
  }

  protected <T> T throwRejected(Object task) {
    throw new RejectedExecutionException("Task " + task + " rejected from " + this);
  }

  protected <T extends Task> T register(T task) {
    tasks.add(task);
    return task;
  }

  protected void unregister(Task task) {
    tasks.remove(task);

    if (!isAlive() && tasks.isEmpty()) terminated.signalAll();
  }

  protected void shutdownTasks(Set<Task> tasks) {
    // do nothing
  }

  protected interface Task<V> extends Runnable, Callable<V>, Future<V> {

    Future<V> getFuture();

    void setFuture(Future<V> future);

    void acquireExecution();

    void releaseExecution();
  }

  protected class RunnableTask<V> implements Task<V> {

    private final Semaphore executionLock = new Semaphore(1);

    private final Runnable runnable;
    private final boolean unregister;

    private Future<V> future;

    public RunnableTask(Runnable runnable, boolean unregister) {
      this.runnable = runnable;
      this.unregister = unregister;
    }

    @Override
    public Future<V> getFuture() {
      return future;
    }

    @Override
    public void setFuture(Future<V> future) {
      this.future = future;
    }

    @Override
    public void acquireExecution() {
      executionLock.acquireUninterruptibly();
    }

    @Override
    public void releaseExecution() {
      executionLock.release();
    }

    @Override
    public void run() {
      acquireExecution();
      try {
        if (!unregister && isCancelled()) return;
        try {
          runnable.run();
        } finally {
          if (unregister) {
            acquireAlive();
            try {
              unregister(this);
            } finally {
              releaseAlive();
            }
          }
        }
      } finally {
        releaseExecution();
      }
    }

    @Override
    public V call() throws Exception {
      run();
      return null;
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return getFuture().cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return getFuture().isCancelled();
    }

    @Override
    public boolean isDone() {
      return getFuture().isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return getFuture().get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return getFuture().get(timeout, unit);
    }
  }

  protected class CallableTask<V> implements Task<V> {

    private final Semaphore executionLock = new Semaphore(1);

    private final Callable<V> callable;
    private final boolean unregister;

    private Future<V> future;

    public CallableTask(Callable callable, boolean unregister) {
      this.callable = callable;
      this.unregister = unregister;
    }

    @Override
    public Future<V> getFuture() {
      return future;
    }

    @Override
    public void setFuture(Future<V> future) {
      this.future = future;
    }

    @Override
    public void acquireExecution() {
      executionLock.acquireUninterruptibly();
    }

    @Override
    public void releaseExecution() {
      executionLock.release();
    }

    @Override
    public void run() {
      acquireExecution();
      try {
        if (!unregister && isCancelled()) return;
        try {
          try {
            callable.call();
          } catch (RuntimeException e) {
            throw e;
          } catch (Exception e) {
            throw new RuntimeException(e);
          }
        } finally {
          if (unregister) {
            acquireAlive();
            try {
              unregister(this);
            } finally {
              releaseAlive();
            }
          }
        }
      } finally {
        releaseExecution();
      }
    }

    @Override
    public V call() throws Exception {
      acquireExecution();
      try {
        if (!unregister && isCancelled()) return null;
        try {
          return callable.call();
        } finally {
          if (unregister) {
            acquireAlive();
            try {
              unregister(this);
            } finally {
              releaseAlive();
            }
          }
        }
      } finally {
        releaseExecution();
      }
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
      return getFuture().cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
      return getFuture().isCancelled();
    }

    @Override
    public boolean isDone() {
      return getFuture().isDone();
    }

    @Override
    public V get() throws InterruptedException, ExecutionException {
      return getFuture().get();
    }

    @Override
    public V get(long timeout, TimeUnit unit)
        throws InterruptedException, ExecutionException, TimeoutException {
      return getFuture().get(timeout, unit);
    }
  }
}
