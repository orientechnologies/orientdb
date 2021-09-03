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
package com.orientechnologies.common.concur.lock;

import com.orientechnologies.common.exception.OException;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OComparableLockManager<T extends Comparable> {
  public enum LOCK {
    SHARED,
    EXCLUSIVE
  }

  private static final int DEFAULT_CONCURRENCY_LEVEL = 16;
  private long acquireTimeout;
  protected final ConcurrentSkipListMap<T, CountableLock> map;
  private final boolean enabled;
  private static final Object NULL_KEY = new Object();

  @SuppressWarnings("serial")
  private static class CountableLock {
    private final AtomicInteger countLocks = new AtomicInteger(1);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  }

  public OComparableLockManager(final boolean iEnabled, final int iAcquireTimeout) {
    this(iEnabled, iAcquireTimeout, defaultConcurrency());
  }

  public OComparableLockManager(
      final boolean iEnabled, final int iAcquireTimeout, final int concurrencyLevel) {
    map = new ConcurrentSkipListMap<T, CountableLock>();

    acquireTimeout = iAcquireTimeout;
    enabled = iEnabled;
  }

  public void acquireSharedLock(final T key) {
    acquireLock(key, LOCK.SHARED);
  }

  public void releaseSharedLock(final T key) {
    releaseLock(Thread.currentThread(), key, LOCK.SHARED);
  }

  public void acquireExclusiveLock(final T key) {
    acquireLock(key, LOCK.EXCLUSIVE);
  }

  public void releaseExclusiveLock(final T key) {
    releaseLock(Thread.currentThread(), key, LOCK.EXCLUSIVE);
  }

  public void acquireLock(final T iResourceId, final LOCK iLockType) {
    acquireLock(iResourceId, iLockType, acquireTimeout);
  }

  public void acquireLock(final T iResourceId, final LOCK iLockType, long iTimeout) {
    if (!enabled) return;

    T immutableResource = getImmutableResourceId(iResourceId);
    if (immutableResource == null) immutableResource = (T) NULL_KEY;

    CountableLock lock;

    while (true) {
      lock = new CountableLock();

      CountableLock oldLock = map.putIfAbsent(immutableResource, lock);
      if (oldLock == null) break;

      lock = oldLock;
      final int oldValue = lock.countLocks.get();

      if (oldValue >= 0) {
        if (lock.countLocks.compareAndSet(oldValue, oldValue + 1)) {
          assert map.get(immutableResource) == lock;
          break;
        }
      } else {
        map.remove(immutableResource, lock);
      }
    }

    try {
      if (iTimeout <= 0) {
        if (iLockType == LOCK.SHARED) lock.readWriteLock.readLock().lock();
        else lock.readWriteLock.writeLock().lock();
      } else {
        try {
          if (iLockType == LOCK.SHARED) {
            if (!lock.readWriteLock.readLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
              throw new OLockException(
                  "Timeout ("
                      + iTimeout
                      + "ms) on acquiring resource '"
                      + iResourceId
                      + "' because is locked from another thread");
          } else {
            if (!lock.readWriteLock.writeLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
              throw new OLockException(
                  "Timeout ("
                      + iTimeout
                      + "ms) on acquiring resource '"
                      + iResourceId
                      + "' because is locked from another thread");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw OException.wrapException(
              new OLockException(
                  "Thread interrupted while waiting for resource '" + iResourceId + "'"),
              e);
        }
      }
    } catch (RuntimeException e) {
      final int usages = lock.countLocks.decrementAndGet();
      if (usages == 0) map.remove(immutableResource);

      throw e;
    }
  }

  public void releaseLock(final Object iRequester, T iResourceId, final LOCK iLockType)
      throws OLockException {
    if (!enabled) return;

    if (iResourceId == null) iResourceId = (T) NULL_KEY;

    final CountableLock lock = map.get(iResourceId);
    if (lock == null)
      throw new OLockException(
          "Error on releasing a non acquired lock by the requester '"
              + iRequester
              + "' against the resource: '"
              + iResourceId
              + "'");

    final int lockCount = lock.countLocks.decrementAndGet();
    if (lockCount == 0) {
      if (lock.countLocks.compareAndSet(0, -1)) {
        map.remove(iResourceId, lock);
      }
    }

    if (iLockType == LOCK.SHARED) lock.readWriteLock.readLock().unlock();
    else lock.readWriteLock.writeLock().unlock();
  }

  // For tests purposes.
  public int getCountCurrentLocks() {
    return map.size();
  }

  protected T getImmutableResourceId(final T iResourceId) {
    return iResourceId;
  }

  private static int defaultConcurrency() {
    if ((Runtime.getRuntime().availableProcessors() << 6) > DEFAULT_CONCURRENCY_LEVEL)
      return Runtime.getRuntime().availableProcessors() << 6;
    else return DEFAULT_CONCURRENCY_LEVEL;
  }

  private static int closestInteger(int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }
}
