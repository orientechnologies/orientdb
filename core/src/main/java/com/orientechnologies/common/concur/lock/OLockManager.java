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

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class OLockManager<RESOURCE_TYPE, REQUESTER_TYPE> {
  public enum LOCK {
    SHARED, EXCLUSIVE
  }

  private static final int                                        DEFAULT_CONCURRENCY_LEVEL = 16;
  protected long                                                  acquireTimeout;
  protected final ConcurrentHashMap<RESOURCE_TYPE, CountableLock> map;
  private final boolean                                           enabled;
  private final int                                               shift;
  private final int                                               mask;
  private final Object[]                                          locks;

  @SuppressWarnings("serial")
  protected static class CountableLock extends ReentrantReadWriteLock {
    protected int countLocks = 0;

    public CountableLock() {
      super(false);
    }
  }

  public OLockManager(final boolean iEnabled, final int iAcquireTimeout) {
    this(iEnabled, iAcquireTimeout, defaultConcurrency());
  }

  public OLockManager(final boolean iEnabled, final int iAcquireTimeout, final int concurrencyLevel) {
    int cL = 1;

    int sh = 0;
    while (cL < concurrencyLevel) {
      cL <<= 1;
      sh++;
    }

    shift = 32 - sh;
    mask = cL - 1;

    map = new ConcurrentHashMap<RESOURCE_TYPE, CountableLock>(cL);
    locks = new Object[cL];
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new Object();
    }

    acquireTimeout = iAcquireTimeout;
    enabled = iEnabled;
  }

  public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType) {
    acquireLock(iRequester, iResourceId, iLockType, acquireTimeout);
  }

  public void acquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType, long iTimeout) {
    if (!enabled)
      return;

    CountableLock lock;
    final Object internalLock = internalLock(iResourceId);
    synchronized (internalLock) {
      lock = map.get(iResourceId);
      if (lock == null) {
        final CountableLock newLock = new CountableLock();
        lock = map.putIfAbsent(getImmutableResourceId(iResourceId), newLock);
        if (lock == null)
          lock = newLock;
      }
      lock.countLocks++;
    }

    try {
      if (iTimeout <= 0) {
        if (iLockType == LOCK.SHARED)
          lock.readLock().lock();
        else
          lock.writeLock().lock();
      } else {
        try {
          if (iLockType == LOCK.SHARED) {
            if (!lock.readLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
              throw new OLockException("Timeout ("+iTimeout+"ms) on acquiring resource '" + iResourceId + "' because is locked from another thread");
          } else {
            if (!lock.writeLock().tryLock(iTimeout, TimeUnit.MILLISECONDS))
              throw new OLockException("Timeout ("+iTimeout+"ms) on acquiring resource '" + iResourceId + "' because is locked from another thread");
          }
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new OLockException("Thread interrupted while waiting for resource '" + iResourceId + "'");
        }
      }
    } catch (RuntimeException e) {
      synchronized (internalLock) {
        lock.countLocks--;
        if (lock.countLocks == 0)
          map.remove(iResourceId);
      }
      throw e;
    }

  }

  public boolean tryAcquireLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType) {
    if (!enabled)
      return true;

    CountableLock lock;
    final Object internalLock = internalLock(iResourceId);
    synchronized (internalLock) {
      lock = map.get(iResourceId);
      if (lock == null) {
        final CountableLock newLock = new CountableLock();
        lock = map.putIfAbsent(getImmutableResourceId(iResourceId), newLock);
        if (lock == null)
          lock = newLock;
      }
      lock.countLocks++;
    }

    boolean result;
    try {
      if (iLockType == LOCK.SHARED)
        result = lock.readLock().tryLock();
      else
        result = lock.writeLock().tryLock();
    } catch (RuntimeException e) {
      synchronized (internalLock) {
        lock.countLocks--;
        if (lock.countLocks == 0)
          map.remove(iResourceId);
      }
      throw e;
    }

    if (!result) {
      synchronized (internalLock) {
        lock.countLocks--;
        if (lock.countLocks == 0)
          map.remove(iResourceId);
      }
    }

    return result;
  }

  public void releaseLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iLockType)
      throws OLockException {
    if (!enabled)
      return;

    final CountableLock lock;
    final Object internalLock = internalLock(iResourceId);
    synchronized (internalLock) {
      lock = map.get(iResourceId);
      if (lock == null)
        throw new OLockException("Error on releasing a non acquired lock by the requester '" + iRequester
            + "' against the resource: '" + iResourceId + "'");

      lock.countLocks--;
      if (lock.countLocks == 0)
        map.remove(iResourceId);
    }
    if (iLockType == LOCK.SHARED)
      lock.readLock().unlock();
    else
      lock.writeLock().unlock();
  }

  public void modifyLock(final REQUESTER_TYPE iRequester, final RESOURCE_TYPE iResourceId, final LOCK iCurrentLockType,
      final LOCK iNewLockType) throws OLockException {
    if (!enabled || iNewLockType == iCurrentLockType)
      return;

    final CountableLock lock;
    final Object internalLock = internalLock(iResourceId);
    synchronized (internalLock) {
      lock = map.get(iResourceId);
      if (lock == null)
        throw new OLockException("Error on releasing a non acquired lock by the requester '" + iRequester
            + "' against the resource: '" + iResourceId + "'");

      if (iCurrentLockType == LOCK.SHARED)
        lock.readLock().unlock();
      else
        lock.writeLock().unlock();

      // RE-ACQUIRE IT
      if (iNewLockType == LOCK.SHARED)
        lock.readLock().lock();
      else
        lock.writeLock().lock();
    }
  }

  public void clear() {
    map.clear();
  }

  // For tests purposes.
  public int getCountCurrentLocks() {
    return map.size();
  }

  public void releaseAllLocksOfRequester(REQUESTER_TYPE iRequester) {
  }

  protected RESOURCE_TYPE getImmutableResourceId(final RESOURCE_TYPE iResourceId) {
    return iResourceId;
  }

  private Object internalLock(final RESOURCE_TYPE iResourceId) {
    final int hashCode = iResourceId.hashCode();
    final int index = (hashCode >>> shift) & mask;
    return locks[index];
  }

  private static int defaultConcurrency() {
    return Runtime.getRuntime().availableProcessors() > DEFAULT_CONCURRENCY_LEVEL ? Runtime.getRuntime().availableProcessors()
        : DEFAULT_CONCURRENCY_LEVEL;
  }
}
