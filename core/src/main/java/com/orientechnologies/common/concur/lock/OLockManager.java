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
package com.orientechnologies.common.concur.lock;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.common.exception.OException;

import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OLockManager<T> {
  public enum LOCK {
    SHARED, EXCLUSIVE
  }

  private static final int DEFAULT_CONCURRENCY_LEVEL = 16;
  private         long                                      acquireTimeout;
  protected final ConcurrentLinkedHashMap<T, CountableLock> map;
  private final   boolean                                   enabled;
  private final   int                                       amountOfCachedInstances;

  @SuppressWarnings("serial")
  private static class CountableLock {
    private final AtomicInteger countLocks    = new AtomicInteger(1);
    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  }

  public OLockManager(final boolean iEnabled, final int iAcquireTimeout, final int amountOfCachedInstances) {
    this(iEnabled, iAcquireTimeout, defaultConcurrency(), amountOfCachedInstances);
  }

  public OLockManager(final boolean iEnabled, final int iAcquireTimeout, final int concurrencyLevel,
      final int amountOfCachedInstances) {

    this.amountOfCachedInstances = amountOfCachedInstances;
    final int cL = closestInteger(concurrencyLevel);

    map = new ConcurrentLinkedHashMap.Builder<T, CountableLock>().concurrencyLevel(cL).maximumWeightedCapacity(Long.MAX_VALUE)
        .build();

    acquireTimeout = iAcquireTimeout;
    enabled = iEnabled;
  }

  public void acquireLock(final T iResourceId, final LOCK iLockType) {
    acquireLock(iResourceId, iLockType, acquireTimeout);
  }

  public void acquireLock(final T iResourceId, final LOCK iLockType, long iTimeout) {
    if (!enabled)
      return;

    if (!enabled)
      return;

    final T immutableResource = getImmutableResourceId(iResourceId);

    CountableLock lock;
    do {
      lock = map.get(immutableResource);

      if (lock != null) {
        final int oldLockCount = lock.countLocks.get();

        if (oldLockCount >= 0) {
          if (lock.countLocks.compareAndSet(oldLockCount, oldLockCount + 1)) {
            break;
          }
        } else {
          map.remove(immutableResource, lock);
        }
      }
    } while (lock != null);

    if (lock == null) {
      while (true) {
        lock = new CountableLock();

        CountableLock oldLock = map.putIfAbsent(immutableResource, lock);
        if (oldLock == null)
          break;

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
    }

    if (map.size() > amountOfCachedInstances) {
      final Iterator<T> keyToRemoveIterator = map.ascendingKeySetWithLimit(1).iterator();
      if (keyToRemoveIterator.hasNext()) {
        final T keyToRemove = keyToRemoveIterator.next();
        final CountableLock lockToRemove = map.get(keyToRemove);
        if (lockToRemove != null) {
          final int counter = lockToRemove.countLocks.get();
          if (counter == 0 && lockToRemove.countLocks.compareAndSet(counter, -1)) {
            assert lockToRemove.countLocks.get() == -1;
            map.remove(keyToRemove, lockToRemove);
          }
        }
      }
    }

    try {
      if (iTimeout <= 0) {
        if (iLockType == LOCK.SHARED)
          lock.readWriteLock.readLock().lock();
        else
          lock.readWriteLock.writeLock().lock();
      } else {
        try {
          if (iLockType == LOCK.SHARED) {
            if (!lock.readWriteLock.readLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
              throw new OLockException(
                  "Timeout (" + iTimeout + "ms) on acquiring resource '" + iResourceId + "' because is locked from another thread");
          } else {
            if (!lock.readWriteLock.writeLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
              throw new OLockException(
                  "Timeout (" + iTimeout + "ms) on acquiring resource '" + iResourceId + "' because is locked from another thread");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw OException
              .wrapException(new OLockException("Thread interrupted while waiting for resource '" + iResourceId + "'"), e);
        }
      }
    } catch (RuntimeException e) {
      final int usages = lock.countLocks.decrementAndGet();
      if (usages == 0)
        map.remove(immutableResource);

      throw e;
    }
  }

  public void releaseLock(final Object iRequester, final T iResourceId, final LOCK iLockType) throws OLockException {
    if (!enabled)
      return;

    final CountableLock lock = map.get(iResourceId);
    if (lock == null)
      throw new OLockException(
          "Error on releasing a non acquired lock by the requester '" + iRequester + "' against the resource: '" + iResourceId
              + "'");

    lock.countLocks.decrementAndGet();

    if (iLockType == LOCK.SHARED)
      lock.readWriteLock.readLock().unlock();
    else
      lock.readWriteLock.writeLock().unlock();
  }

  // For tests purposes.
  public int getCountCurrentLocks() {
    return map.size();
  }

  protected T getImmutableResourceId(final T iResourceId) {
    return iResourceId;
  }

  private static int defaultConcurrency() {
    return (Runtime.getRuntime().availableProcessors() << 6) > DEFAULT_CONCURRENCY_LEVEL ?
        (Runtime.getRuntime().availableProcessors() << 6) :
        DEFAULT_CONCURRENCY_LEVEL;
  }

  private static int closestInteger(int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }
}
