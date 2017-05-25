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
package com.orientechnologies.common.concur.lock;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Basically the same thing as {@link OOneEntryPerKeyLockManager}, but uses {@link ConcurrentHashMap} internally which has better
 * memory footprint.
 */
public class OIndexOneEntryPerKeyLockManager<T> implements OLockManager<T> {

  private final static Object NULL_KEY = new Object();

  private final ConcurrentHashMap<T, CountableLock> locks;

  public OIndexOneEntryPerKeyLockManager() {
    this(OGlobalConfiguration.ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL.getValueAsInteger());
  }

  public OIndexOneEntryPerKeyLockManager(int concurrencyLevel) {
    final int ceilingConcurrencyLevel = ceilingPowerOf2(concurrencyLevel);
    locks = new ConcurrentHashMap<T, CountableLock>(16, 0.75F, ceilingConcurrencyLevel);
  }

  @Override
  public Lock acquireSharedLock(T key) {
    return acquireLock(key, true);
  }

  @Override
  public void releaseSharedLock(T key) {
    releaseLock(key, true);
  }

  @Override
  public Lock acquireExclusiveLock(T key) {
    return acquireLock(key, false);
  }

  @Override
  public void releaseExclusiveLock(T key) {
    releaseLock(key, false);
  }

  @Override
  public Lock[] acquireSharedLocksInBatch(T... keys) {
    return acquireLockInBatch(keys, true);
  }

  @Override
  public Lock[] acquireExclusiveLocksInBatch(T... keys) {
    return acquireLockInBatch(keys, false);
  }

  @Override
  public Lock[] acquireExclusiveLocksInBatch(Collection<T> keys) {
    if (keys == null || keys.isEmpty())
      return new Lock[0];

    final List<Comparable> comparables = new ArrayList<Comparable>();

    int seenNulls = 0;
    for (T key : keys) {
      if (key instanceof Comparable) {
        comparables.add((Comparable) key);
      } else if (key == null) {
        ++seenNulls;
      } else {
        throw new IllegalArgumentException(
            "In order to lock a key in a batch it should implement " + Comparable.class.getName() + " interface");
      }
    }

    //noinspection unchecked
    Collections.sort(comparables);

    final Lock[] locks = new Lock[comparables.size() + seenNulls];
    int i = 0;
    for (int j = 0; j < seenNulls; ++j)
      //noinspection unchecked
      locks[i++] = acquireExclusiveLock((T) NULL_KEY);
    for (Comparable key : comparables)
      //noinspection unchecked
      locks[i++] = acquireExclusiveLock((T) key);

    return locks;
  }

  @Override
  public void lockAllExclusive() {
    for (CountableLock lock : locks.values()) {
      lock.readWriteLock.writeLock().lock();
    }
  }

  @Override
  public void unlockAllExclusive() {
    for (CountableLock lock : locks.values()) {
      lock.readWriteLock.writeLock().unlock();
    }
  }

  private Lock acquireLock(T key, boolean read) {
    key = immutalizeKey(key);

    if (key == null)
      //noinspection unchecked
      key = (T) NULL_KEY;

    CountableLock lock;
    do {
      lock = locks.get(key);

      if (lock != null) {
        final int oldLevel = lock.level.get();

        if (oldLevel >= 0) {
          if (lock.level.compareAndSet(oldLevel, oldLevel + 1))
            break;
        } else {
          locks.remove(key, lock);
        }
      }
    } while (lock != null);

    if (lock == null) {
      while (true) {
        lock = new CountableLock();

        CountableLock oldLock = locks.putIfAbsent(key, lock);
        if (oldLock == null)
          break;

        lock = oldLock;
        final int oldLevel = lock.level.get();

        if (oldLevel >= 0) {
          if (lock.level.compareAndSet(oldLevel, oldLevel + 1)) {
            assert locks.get(key) == lock;
            break;
          }
        } else {
          locks.remove(key, lock);
        }
      }
    }

    if (read)
      lock.readWriteLock.readLock().lock();
    else
      lock.readWriteLock.writeLock().lock();

    return new CountableLockWrapper<T>(key, lock, locks, read);
  }

  private void releaseLock(T key, boolean read) throws OLockException {
    key = immutalizeKey(key);

    if (key == null)
      //noinspection unchecked
      key = (T) NULL_KEY;

    final CountableLock lock = locks.get(key);
    if (lock == null)
      throw new OLockException(
          "Error on releasing a non acquired lock by thread '" + Thread.currentThread() + "' against the resource: '" + key + "'");

    if (lock.level.decrementAndGet() == 0 && lock.level.compareAndSet(0, -1)) {
      assert lock.level.get() == -1;
      locks.remove(key, lock);
    }

    if (read)
      lock.readWriteLock.readLock().unlock();
    else
      lock.readWriteLock.writeLock().unlock();
  }

  private Lock[] acquireLockInBatch(T[] keys, boolean read) {
    if (keys == null || keys.length == 0)
      return null;

    final List<Comparable> comparables = new ArrayList<Comparable>();

    int seenNulls = 0;
    for (T key : keys) {
      if (key instanceof Comparable) {
        comparables.add((Comparable) key);
      } else if (key == null) {
        ++seenNulls;
      } else {
        throw new IllegalArgumentException(
            "In order to lock a key in a batch it should implement " + Comparable.class.getName() + " interface");
      }
    }

    //noinspection unchecked
    Collections.sort(comparables);

    final Lock[] locks = new Lock[comparables.size() + seenNulls];
    int i = 0;
    for (int j = 0; j < seenNulls; ++j)
      //noinspection unchecked
      locks[i++] = read ? acquireSharedLock((T) NULL_KEY) : acquireExclusiveLock((T) NULL_KEY);
    for (Comparable key : comparables)
      //noinspection unchecked
      locks[i++] = read ? acquireSharedLock((T) key) : acquireExclusiveLock((T) key);

    return locks;
  }

  private T immutalizeKey(T key) {
    if (key instanceof OIdentifiable) {
      //noinspection unchecked
      return (T) ((OIdentifiable) key).getIdentity().copy();
    } else if (key instanceof OCompositeKey) {
      final OCompositeKey compositeKey = (OCompositeKey) key;

      boolean needsCopy = false;
      for (Object subkey : compositeKey.getKeys()) {
        assert !(subkey instanceof OCompositeKey);

        if (subkey instanceof OIdentifiable) {
          needsCopy = true;
          break;
        }
      }

      if (needsCopy) {
        final OCompositeKey copy = new OCompositeKey();
        for (Object subkey : compositeKey.getKeys())
          copy.addKey(subkey instanceof OIdentifiable ? ((OIdentifiable) subkey).getIdentity().copy() : subkey);

        //noinspection unchecked
        return (T) copy;
      } else
        return key;
    } else
      return key;
  }

  public int getLockCount() { // for testing purposes only
    return locks.size();
  }

  private static int ceilingPowerOf2(int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }

  private static class CountableLock {
    public final AtomicInteger level         = new AtomicInteger(1);
    public final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  }

  @SuppressWarnings("NullableProblems")
  private static class CountableLockWrapper<T> implements Lock {

    private final T                                   key;
    private final CountableLock                       lock;
    private final ConcurrentHashMap<T, CountableLock> locks;
    private final boolean                             read;

    public CountableLockWrapper(T key, CountableLock lock, ConcurrentHashMap<T, CountableLock> locks, boolean read) {
      this.key = key;
      this.lock = lock;
      this.locks = locks;
      this.read = read;
    }

    @Override
    public void lock() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) throws InterruptedException {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unlock() {
      assert lock == locks.get(key);

      if (lock.level.decrementAndGet() == 0 && lock.level.compareAndSet(0, -1)) {
        assert lock.level.get() == -1;
        locks.remove(key, lock);
      }

      if (read)
        lock.readWriteLock.readLock().unlock();
      else
        lock.readWriteLock.writeLock().unlock();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }

}
