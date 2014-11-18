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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.concur.OTimeoutException;

/**
 * Shared resource. Sub classes can acquire and release shared and exclusive locks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSharedResourceTimeout {
  private final ReadWriteLock lock = new ReentrantReadWriteLock();
  protected int               timeout;

  public OSharedResourceTimeout(final int timeout) {
    this.timeout = timeout;
  }

  protected void acquireSharedLock() throws OTimeoutException {
    try {
      if (timeout == 0) {
        lock.readLock().lock();
        return;
      } else if (lock.readLock().tryLock(timeout, TimeUnit.MILLISECONDS))
        // OK
        return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    throwTimeoutException(lock.readLock());
  }

  protected void releaseSharedLock() {
    lock.readLock().unlock();
  }

  protected void acquireExclusiveLock() throws OTimeoutException {
    try {
      if (timeout == 0) {
        lock.writeLock().lock();
        return;
      } else if (lock.writeLock().tryLock(timeout, TimeUnit.MILLISECONDS))
        // OK
        return;
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
    }

    throwTimeoutException(lock.writeLock());
  }

  protected void releaseExclusiveLock() {
    lock.writeLock().unlock();
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
