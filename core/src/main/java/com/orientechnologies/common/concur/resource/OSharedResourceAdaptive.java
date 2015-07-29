/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.common.concur.resource;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OLockException;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Adaptive class to handle shared resources. It's configurable specifying if it's running in a concurrent environment and allow o
 * specify a maximum timeout to avoid deadlocks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSharedResourceAdaptive {
  private final ReentrantReadWriteLock lock          = new ReentrantReadWriteLock();
  private final AtomicInteger          users         = new AtomicInteger(0);
  private final boolean                concurrent;
  private final int                    timeout;
  private final boolean                ignoreThreadInterruption;
  private int                          scaledUpCount = 0;

  protected OSharedResourceAdaptive() {
    this.concurrent = true;
    this.timeout = 0;
    this.ignoreThreadInterruption = false;
  }

  protected OSharedResourceAdaptive(final int iTimeout) {
    this.concurrent = true;
    this.timeout = iTimeout;
    this.ignoreThreadInterruption = false;
  }

  protected OSharedResourceAdaptive(final boolean iConcurrent) {
    this.concurrent = iConcurrent;
    this.timeout = 0;
    this.ignoreThreadInterruption = false;
  }

  protected OSharedResourceAdaptive(final boolean iConcurrent, final int iTimeout, boolean ignoreThreadInterruption) {
    this.concurrent = iConcurrent;
    this.timeout = iTimeout;
    this.ignoreThreadInterruption = ignoreThreadInterruption;
  }

  public int getUsers() {
    return users.get();
  }

  public int addUser() {
    return users.incrementAndGet();
  }

  public int removeUser() {
    if (users.get() < 1)
      throw new IllegalStateException("Cannot remove user of the shared resource " + toString() + " because no user is using it");

    return users.decrementAndGet();
  }

  public boolean isConcurrent() {
    return concurrent;
  }

  /** To use in assert block. */
  public boolean assertExclusiveLockHold() {
    return lock.getWriteHoldCount() > 0;
  }

  /** To use in assert block. */
  public boolean assertSharedLockHold() {
    return lock.getReadHoldCount() > 0;
  }

  protected void acquireExclusiveLock() {
    if (concurrent)
      if (timeout > 0) {
        try {

          if (lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS))
            // OK
            return;
        } catch (InterruptedException e) {
          if (ignoreThreadInterruption) {
            // IGNORE THE THREAD IS INTERRUPTED: TRY TO RE-LOCK AGAIN
            try {
              if (lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
                // OK, RESET THE INTERRUPTED STATE
                Thread.currentThread().interrupt();
                return;
              }
            } catch (InterruptedException e2) {
              Thread.currentThread().interrupt();
            }
          }

          throw new OLockException("Thread interrupted while waiting for resource of class '" + getClass() + "' with timeout="
              + timeout, e);
        }
        throwTimeoutException(lock.writeLock());
      } else {
        lock.writeLock().lock();
      }
  }

  protected boolean tryAcquireExclusiveLock() {
    return !concurrent || lock.writeLock().tryLock();
  }

  protected void acquireSharedLock() {
    if (concurrent)
      if (timeout > 0) {
        try {
          if (lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS))
            // OK
            return;
        } catch (InterruptedException e) {
          if (ignoreThreadInterruption) {
            // IGNORE THE THREAD IS INTERRUPTED: TRY TO RE-LOCK AGAIN
            try {
              if (lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS)) {
                // OK, RESET THE INTERRUPTED STATE
                Thread.currentThread().interrupt();
                return;
              }
            } catch (InterruptedException e2) {
              Thread.currentThread().interrupt();
            }
          }
          throw new OLockException("Thread interrupted while waiting for resource of class '" + getClass() + "' with timeout="
              + timeout, e);
        }

        throwTimeoutException(lock.readLock());
      } else
        lock.readLock().lock();
  }

  protected boolean tryAcquireSharedLock() {
    return !concurrent || lock.readLock().tryLock();
  }

  protected void releaseExclusiveLock() {
    if (concurrent) {
      lock.writeLock().unlock();
    }
  }

  protected void releaseSharedLock() {
    if (concurrent)
      lock.readLock().unlock();
  }

  private void throwTimeoutException(Lock lock) {
    final String owner = extractLockOwnerStackTrace(lock);

    throw new OTimeoutException("Timeout on acquiring exclusive lock against resource of class: " + getClass() + " with timeout="
        + timeout + (owner != null ? "\n" + owner : ""));
  }

  private String extractLockOwnerStackTrace(Lock lock) {
    try {
      Field syncField = lock.getClass().getDeclaredField("sync");
      syncField.setAccessible(true);

      Object sync = syncField.get(lock);
      Method getOwner = sync.getClass().getSuperclass().getDeclaredMethod("getOwner");
      getOwner.setAccessible(true);

      final Thread owner = (Thread) getOwner.invoke(sync);
      if (owner == null)
        return null;

      StringWriter stringWriter = new StringWriter();
      PrintWriter printWriter = new PrintWriter(stringWriter);

      printWriter.append("Owner thread : ").append(owner.toString()).append("\n");

      StackTraceElement[] stackTrace = owner.getStackTrace();
      for (StackTraceElement traceElement : stackTrace)
        printWriter.println("\tat " + traceElement);

      printWriter.flush();
      return stringWriter.toString();
    } catch (RuntimeException e) {
      return null;
    } catch (NoSuchFieldException e) {
      return null;
    } catch (IllegalAccessException e) {
      return null;
    } catch (NoSuchMethodException e) {
      return null;
    } catch (InvocationTargetException e) {
      return null;
    }

  }
}
