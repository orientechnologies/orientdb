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

import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 8/11/14
 */
public class ONewLockManager<T> {
  private static final int MAXIMUM_CAPACITY = 1 << 30;

  private final int mask;

  private final ReadWriteLock[]          locks;
  private final OReadersWriterSpinLock[] spinLocks;

  private final boolean useSpinLock;

  private static final class SpinLockWrapper implements Lock {
    private final boolean                readLock;
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
      if (readLock)
        spinLock.releaseReadLock();
      else
        spinLock.releaseWriteLock();
    }

    @Override
    public Condition newCondition() {
      throw new UnsupportedOperationException();
    }
  }

  public ONewLockManager(int concurrencyLevel) {
    this(false, concurrencyLevel);
  }

  public ONewLockManager(boolean useSpinLock, int concurrencyLevel) {

    this.useSpinLock = useSpinLock;
    final int tableSize = tableSizeFor(concurrencyLevel << 4);
    mask = tableSize - 1;

    if (useSpinLock) {
      OReadersWriterSpinLock[] lcks = new OReadersWriterSpinLock[tableSize];

      for (int i = 0; i < lcks.length; i++)
        lcks[i] = new OReadersWriterSpinLock();

      spinLocks = lcks;
      locks = null;
    } else {
      ReadWriteLock[] lcks = new ReadWriteLock[tableSize];
      for (int i = 0; i < lcks.length; i++)
        lcks[i] = new ReentrantReadWriteLock();

      locks = lcks;
      spinLocks = null;
    }
  }

  private static int longHashCode(long value) {
    return (int) (value ^ (value >>> 32));
  }

  private int index(int hashCode) {
    return hashCode & mask;
  }

  public Lock acquireExclusiveLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireWriteLock();
      return new SpinLockWrapper(false, spinLock);
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();

    return lock;
  }

  public Lock acquireExclusiveLock(int value) {
    final int index = index(value);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireWriteLock();

      return new SpinLockWrapper(false, spinLock);
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();
    return lock;
  }

  public Lock acquireExclusiveLock(T value) {
    final int index;
    if (value == null)
      index = 0;
    else
      index = index(value.hashCode());

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireWriteLock();

      return new SpinLockWrapper(false, spinLock);
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();
    return lock;
  }

  public void lockAllExclusive() {
    if (useSpinLock) {
      for (OReadersWriterSpinLock spinLock : spinLocks) {
        spinLock.acquireWriteLock();
      }
    } else {
      for (ReadWriteLock readWriteLock : locks) {
        readWriteLock.writeLock().lock();
      }
    }
  }

  public void unlockAllExclusive() {
    if (useSpinLock) {
      for (OReadersWriterSpinLock spinLock : spinLocks) {
        spinLock.releaseWriteLock();
      }
    } else {
      for (ReadWriteLock readWriteLock : locks) {
        readWriteLock.writeLock().unlock();
      }
    }
  }

  public boolean tryAcquireExclusiveLock(T value, long timeout) throws InterruptedException {
    if (useSpinLock)
      throw new IllegalStateException("Spin lock does not support try lock mode");

    final int index;
    if (value == null)
      index = 0;
    else
      index = index(value.hashCode());

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
  }

  public void acquireExclusiveLocksInBatch(T... value) {
    if (value == null)
      return;

    final T[] values = Arrays.copyOf(value, value.length);

    Arrays.sort(values, 0, values.length, new Comparator<T>() {
      @Override
      public int compare(T one, T two) {
        final int indexOne;
        if (one == null)
          indexOne = 0;
        else
          indexOne = index(one.hashCode());

        final int indexTwo;
        if (two == null)
          indexTwo = 0;
        else
          indexTwo = index(two.hashCode());

        if (indexOne > indexTwo)
          return 1;

        if (indexOne < indexTwo)
          return -1;

        return 0;
      }
    });

    for (T val : values) {
      acquireExclusiveLock(val);
    }
  }

  public void acquireExclusiveLocksInBatch(Collection<T> values) {
    if (values == null)
      return;

    final List<T> valCopy = new ArrayList<T>(values);
    Collections.sort(valCopy, new Comparator<T>() {
      @Override
      public int compare(T one, T two) {
        final int indexOne;
        if (one == null)
          indexOne = 0;
        else
          indexOne = index(one.hashCode());

        final int indexTwo;
        if (two == null)
          indexTwo = 0;
        else
          indexTwo = index(two.hashCode());

        if (indexOne > indexTwo)
          return 1;

        if (indexOne < indexTwo)
          return -1;

        return 0;
      }
    });

    for (T val : valCopy) {
      acquireExclusiveLock(val);
    }
  }

  public Lock acquireSharedLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireReadLock();

      return new SpinLockWrapper(true, spinLock);
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();

    return lock;

  }

  public boolean tryAcquireSharedLock(T value, long timeout) throws InterruptedException {
    if (useSpinLock)
      throw new IllegalStateException("Spin lock does not support try lock mode");

    final int index;
    if (value == null)
      index = 0;
    else
      index = index(value.hashCode());

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    return lock.tryLock(timeout, TimeUnit.MILLISECONDS);
  }

  public Lock acquireSharedLock(int value) {
    final int index = index(value);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireReadLock();

      return new SpinLockWrapper(true, spinLock);
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();
    return lock;
  }

  public Lock acquireSharedLock(T value) {
    final int index;
    if (value == null)
      index = 0;
    else
      index = index(value.hashCode());

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.acquireReadLock();

      return new SpinLockWrapper(true, spinLock);
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();
    return lock;
  }

  public void releaseSharedLock(int value) {
    final int index = index(value);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseReadLock();
      return;
    }

    final ReadWriteLock rwLock = locks[index];
    rwLock.readLock().unlock();
  }

  public void releaseSharedLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseReadLock();
      return;
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.unlock();
  }

  public void releaseSharedLock(T value) {
    final int index;
    if (value == null)
      index = 0;
    else
      index = index(value.hashCode());

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseReadLock();
      return;
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.unlock();
  }

  public void releaseExclusiveLock(int value) {
    final int index = index(value);
    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseWriteLock();
      return;
    }

    final ReadWriteLock rwLock = locks[index];
    rwLock.writeLock().unlock();
  }

  public void releaseExclusiveLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseWriteLock();
      return;
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.unlock();
  }

  public void releaseExclusiveLock(T value) {
    final int index;
    if (value == null)
      index = 0;
    else
      index = index(value.hashCode());

    if (useSpinLock) {
      OReadersWriterSpinLock spinLock = spinLocks[index];
      spinLock.releaseWriteLock();
      return;
    }

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.unlock();
  }

  public void releaseLock(Lock lock) {
    lock.unlock();
  }

  /**
   * Returns a power of two table size for the given desired capacity.
   * See Hackers Delight, sec 3.2
   */
  private static int tableSizeFor(int c) {
    int n = c - 1;
    n |= n >>> 1;
    n |= n >>> 2;
    n |= n >>> 4;
    n |= n >>> 8;
    n |= n >>> 16;
    return (n < 0) ? 1 : (n >= MAXIMUM_CAPACITY) ? MAXIMUM_CAPACITY : n + 1;
  }

}
