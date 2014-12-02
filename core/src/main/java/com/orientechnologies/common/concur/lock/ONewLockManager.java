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
  private static final int               CONCURRENCY_LEVEL = closestInteger(Runtime.getRuntime().availableProcessors() * 64);
  private static final int               MASK              = CONCURRENCY_LEVEL - 1;

  private final ReadWriteLock[]          locks;
  private final OReadersWriterSpinLock[] spinLocks;

  private final boolean                  useSpinLock;

  private static int closestInteger(int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }

  public ONewLockManager() {
    this(false);
  }

  public ONewLockManager(boolean useSpinLock) {
    this.useSpinLock = useSpinLock;
    if (useSpinLock) {
      OReadersWriterSpinLock[] lcks = new OReadersWriterSpinLock[CONCURRENCY_LEVEL];

      for (int i = 0; i < lcks.length; i++)
        lcks[i] = new OReadersWriterSpinLock();

      spinLocks = lcks;
      locks = null;
    } else {
      ReadWriteLock[] lcks = new ReadWriteLock[CONCURRENCY_LEVEL];
      for (int i = 0; i < lcks.length; i++)
        lcks[i] = new ReentrantReadWriteLock();

      locks = lcks;
      spinLocks = null;
    }
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
    final int index = index(value.hashCode());

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

  public Lock tryAcquireExclusiveLock(T value, long timeOut) throws InterruptedException {
    final int index = index(value.hashCode());

    if (useSpinLock)
      throw new UnsupportedOperationException();

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    if (lock.tryLock(timeOut, TimeUnit.MILLISECONDS))
      return lock;

    return null;
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
    final int index = index(value.hashCode());

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
    final int index = index(value.hashCode());

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
    final int index = index(value.hashCode());

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

  private static int longHashCode(long value) {
    return (int) (value ^ (value >>> 32));
  }

  private static int index(int hashCode) {
    return hashCode & MASK;
  }

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

}
