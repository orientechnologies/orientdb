/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.common.concur.resource;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.common.concur.lock.OLockException;

/**
 * Adaptive class to handle shared resources. It's configurable specifying if it's running in a concurrent environment and allow o
 * specify a maximum timeout to avoid deadlocks.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OAdaptiveLock {
  private final ReentrantLock lock = new ReentrantLock();
  private final boolean       concurrent;
  private final int           timeout;
  private final boolean       ignoreThreadInterruption;

  public OAdaptiveLock() {
    this.concurrent = true;
    this.timeout = 0;
    this.ignoreThreadInterruption = false;
  }

  public OAdaptiveLock(final int iTimeout) {
    this.concurrent = true;
    this.timeout = iTimeout;
    this.ignoreThreadInterruption = false;
  }

  public OAdaptiveLock(final boolean iConcurrent) {
    this.concurrent = iConcurrent;
    this.timeout = 0;
    this.ignoreThreadInterruption = false;
  }

  public OAdaptiveLock(final boolean iConcurrent, final int iTimeout, boolean ignoreThreadInterruption) {
    this.concurrent = iConcurrent;
    this.timeout = iTimeout;
    this.ignoreThreadInterruption = ignoreThreadInterruption;
  }

  public void lock() {
    if (concurrent)
      if (timeout > 0) {
        try {
          if (lock.tryLock(timeout, TimeUnit.MILLISECONDS))
            // OK
            return;
        } catch (InterruptedException e) {
          if (ignoreThreadInterruption) {
            // IGNORE THE THREAD IS INTERRUPTED: TRY TO RE-LOCK AGAIN
            try {
              if (lock.tryLock(timeout, TimeUnit.MILLISECONDS)) {
                // OK, RESET THE INTERRUPTED STATE
                Thread.currentThread().interrupt();
                return;
              }
            } catch (InterruptedException e2) {
              Thread.currentThread().interrupt();
            }
          }

          throw new OLockException("Thread interrupted while waiting for resource of class '" + getClass() + "' with timeout="
              + timeout);
        }
        throw new OTimeoutException("Timeout on acquiring  lock against resource of class: " + getClass() + " with timeout="
            + timeout);
      } else
        lock.lock();
  }

  public boolean tryAcquireLock() {
    return concurrent || lock.tryLock();
  }

  public void unlock() {
    if (concurrent)
      lock.unlock();
  }

  public boolean isConcurrent() {
    return concurrent;
  }

  public ReentrantLock getUnderlying() {
    return lock;
  }
}
