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

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Lock manager implementation that uses multipel partitions to increase the level of concurrency
 * without having to keep one entry per locked key, like for {@link OOneEntryPerKeyLockManager}
 * implementation.
 *
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/11/14
 */
public class OPartitionedLockManager<T> implements OLockManager<T> {
  private static final int HASH_BITS = 0x7fffffff;

  private final int concurrencyLevel =
      closestInteger(
          OGlobalConfiguration.ENVIRONMENT_LOCK_MANAGER_CONCURRENCY_LEVEL.getValueAsInteger());
  private final int mask = concurrencyLevel - 1;

  private final ReadWriteLock[] locks;
  private final OReadersWriterSpinLock[] spinLocks;
  private final ScalableRWLock[] scalableRWLocks;

  private final boolean useSpinLock;
  private final boolean useScalableRWLock;
  private final Comparator comparator =
      (one, two) -> {
        final int indexOne;
        if (one == null) indexOne = 0;
        else indexOne = index(one.hashCode());

        final int indexTwo;
        if (two == null) indexTwo = 0;
        else indexTwo = index(two.hashCode());

        return Integer.compare(indexOne, indexTwo);
      };

  private static final class SpinLockWrapper implements Lock {
    private final boolean readLock;
    private final OReadersWriterSpinLock spinLock;

    private SpinLockWrapper(boolean readLock, OReadersWriterSpinLock spinLock) {
      this.readLock = readLock;
      this.spinLock = spinLock;
    }

    @Override
    public void lock() {
      throw new UnsupportedOperationException();
    }

    @Override
    public void lockInterruptibly() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock() {
      throw new UnsupportedOperationException();
    }

    @Override
    public boolean tryLock(long time, TimeUnit unit) {
      throw new UnsupportedOperationException();
    }

    @Override
    public void unlock() {
      if (readLock) spinLock.releaseReadLock();
      else spinLock.releaseWriteLock();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }

  public OPartitionedLockManager() {
    this(false, false);
  }

  public OPartitionedLockManager(boolean useSpinLock, boolean useScalableRWLock) {
    this.useSpinLock = useSpinLock;
    this.useScalableRWLock = useScalableRWLock;

    if (this.useScalableRWLock && this.useSpinLock) {
      throw new IllegalArgumentException(
          "Spinlock and scalable RW lock can not be used simultaneously");
    }

    if (useSpinLock) {
      OReadersWriterSpinLock[] lcks = new OReadersWriterSpinLock[concurrencyLevel];

      for (int i = 0; i < lcks.length; i++) lcks[i] = new OReadersWriterSpinLock();

      spinLocks = lcks;
      locks = null;
      scalableRWLocks = null;
    } else if (useScalableRWLock) {
      ScalableRWLock[] lcks = new ScalableRWLock[concurrencyLevel];
      for (int i = 0; i < lcks.length; i++) {
        lcks[i] = new ScalableRWLock();
      }

      spinLocks = null;
      locks = null;
      scalableRWLocks = lcks;
    } else {
      ReadWriteLock[] lcks = new ReadWriteLock[concurrencyLevel];
      for (int i = 0; i < lcks.length; i++) lcks[i] = new ReentrantReadWriteLock();

      locks = lcks;
      spinLocks = null;
      scalableRWLocks = null;
    }
  }

  private static int closestInteger(int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }

  private static int longHashCode(long value) {
    return (int) (value ^ (value >>> 32));
  }

  private int index(int hashCode) {
    return shuffleHashCode(hashCode) & mask;
  }

  private static int shuffleHashCode(int h) {
    return (h ^ (h >>> 16)) & HASH_BITS;
  }

  public Lock acquireExclusiveLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    if (useSpinLock) {
      assert spinLocks != null;
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireWriteLock();
      return new SpinLockWrapper(false, spinLock);
    }

    if (useScalableRWLock) {
      return scalableExclusiveLock(index);
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();

    return lock;
  }

  private Lock scalableExclusiveLock(int index) {
    assert scalableRWLocks != null;
    final ScalableRWLock scalableRWLock = scalableRWLocks[index];
    final Lock lock = scalableRWLock.writeLock();
    lock.lock();
    return lock;
  }

  private Lock scalableSharedLock(int index) {
    assert scalableRWLocks != null;
    final ScalableRWLock scalableRWLock = scalableRWLocks[index];
    final Lock lock = scalableRWLock.readLock();
    lock.lock();
    return lock;
  }

  public Lock acquireExclusiveLock(int value) {
    final int index = index(value);

    if (useSpinLock) {
      assert spinLocks != null;
      final OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireWriteLock();

      return new SpinLockWrapper(false, spinLock);
    }

    if (useScalableRWLock) {
      return scalableExclusiveLock(index);
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();
    return lock;
  }

  @Override
  public Lock acquireExclusiveLock(T value) {
    final int index;
    if (value == null) index = 0;
    else index = index(value.hashCode());

    if (useSpinLock) {
      assert spinLocks != null;

      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireWriteLock();

      return new SpinLockWrapper(false, spinLock);
    }

    if (useScalableRWLock) {
      return scalableExclusiveLock(index);
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();

    lock.lock();
    return lock;
  }

  public boolean tryAcquireExclusiveLock(final int value, final long timeout)
      throws InterruptedException {
    if (useSpinLock) throw new IllegalStateException("Spin lock does not support try lock mode");

    final int index = index(value);

    if (useScalableRWLock) {
      assert scalableRWLocks != null;

      final ScalableRWLock scalableRWLock = scalableRWLocks[index];
      return scalableRWLock.exclusiveTryLock();
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
  }

  @SafeVarargs
  @Override
  public final Lock[] acquireExclusiveLocksInBatch(final T... value) {
    if (value == null) return new Lock[0];

    final Lock[] locks = new Lock[value.length];
    final T[] sortedValues = getOrderedValues(value);

    for (int n = 0; n < sortedValues.length; n++) {
      locks[n] = acquireExclusiveLock(sortedValues[n]);
    }

    return locks;
  }

  @SafeVarargs
  public final Lock[] acquireSharedLocksInBatch(final T... value) {
    if (value == null) return new Lock[0];

    final Lock[] locks = new Lock[value.length];
    final T[] sortedValues = getOrderedValues(value);

    for (int i = 0; i < sortedValues.length; i++) {
      locks[i] = acquireSharedLock(sortedValues[i]);
    }

    return locks;
  }

  public Lock[] acquireExclusiveLocksInBatch(Collection<T> values) {
    if (values == null || values.isEmpty()) return new Lock[0];

    final Collection<T> valCopy = getOrderedValues(values);

    final Lock[] locks = new Lock[values.size()];
    int i = 0;
    for (T val : valCopy) {
      locks[i++] = acquireExclusiveLock(val);
    }
    return locks;
  }

  public Lock[] acquireExclusiveLocksInBatch(int[] values) {
    if (values == null || values.length == 0) {
      return new Lock[0];
    }

    final int[] orderedValues = new int[values.length];
    Arrays.sort(orderedValues);

    final Lock[] locks = new Lock[orderedValues.length];
    int i = 0;
    for (int val : orderedValues) {
      locks[i++] = acquireExclusiveLock(val);
    }

    return locks;
  }

  public Lock acquireSharedLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    return sharedLock(index);
  }

  private Lock sharedLock(int index) {
    if (useSpinLock) {
      assert spinLocks != null;

      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireReadLock();

      return new SpinLockWrapper(true, spinLock);
    }

    if (useScalableRWLock) {
      return scalableSharedLock(index);
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();

    return lock;
  }

  public Lock acquireSharedLock(int value) {
    final int index = index(value);
    return sharedLock(index);
  }

  @Override
  public Lock acquireSharedLock(final T value) {
    final int index;
    if (value == null) index = 0;
    else index = index(value.hashCode());

    return sharedLock(index);
  }

  public void releaseSharedLock(final int value) {
    final int index = index(value);

    releaseSLock(index);
  }

  private void releaseSLock(int index) {
    if (useSpinLock) {
      assert spinLocks != null;
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseReadLock();
      return;
    }

    if (useScalableRWLock) {
      assert scalableRWLocks != null;

      final ScalableRWLock scalableRWLock = scalableRWLocks[index];
      scalableRWLock.sharedLock();
      return;
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];
    rwLock.readLock().unlock();
  }

  public void releaseSharedLock(final long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    releaseSLock(index);
  }

  public void releaseSharedLock(final T value) {
    final int index;
    if (value == null) index = 0;
    else index = index(value.hashCode());

    releaseSLock(index);
  }

  public void releaseExclusiveLock(final int value) {
    final int index = index(value);
    releaseWLock(index);
  }

  private void releaseWLock(int index) {
    if (useSpinLock) {
      assert spinLocks != null;
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseWriteLock();
      return;
    }

    if (useScalableRWLock) {
      assert scalableRWLocks != null;

      final ScalableRWLock scalableRWLock = scalableRWLocks[index];
      scalableRWLock.exclusiveUnlock();
      return;
    }

    assert locks != null;
    final ReadWriteLock rwLock = locks[index];
    rwLock.writeLock().unlock();
  }

  public void releaseExclusiveLock(final long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    releaseWLock(index);
  }

  public void releaseExclusiveLock(final T value) {
    final int index;
    if (value == null) index = 0;
    else index = index(value.hashCode());

    releaseWLock(index);
  }

  private T[] getOrderedValues(final T[] values) {
    if (values.length < 2) {
      // OPTIMIZED VERSION WITH JUST 1 ITEM (THE MOST COMMON)
      return values;
    }

    final T[] copy = Arrays.copyOf(values, values.length);

    //noinspection unchecked
    Arrays.sort(copy, 0, copy.length, comparator);

    return copy;
  }

  private Collection<T> getOrderedValues(final Collection<T> values) {
    if (values.size() < 2) {
      // OPTIMIZED VERSION WITH JUST 1 ITEM (THE MOST COMMON)
      return values;
    }

    final List<T> valCopy = new ArrayList<T>(values);
    //noinspection unchecked
    valCopy.sort(comparator);

    return valCopy;
  }
}
