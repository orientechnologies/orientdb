/*
 * Copyright 2010-2014 Luca Garulli (l.garulli--at--orientechnologies.com)
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

package com.orientechnologies.common.concur.lock;

import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 8/11/14
 */
public class ONewLockManager<T> {
  private static final int      CONCURRENCY_LEVEL = closestInteger(Runtime.getRuntime().availableProcessors() * 64);
  private static final int      MASK              = CONCURRENCY_LEVEL - 1;

  private final ReadWriteLock[] locks             = new ReadWriteLock[CONCURRENCY_LEVEL];

  private static int closestInteger(int value) {
    return 1 << (32 - Integer.numberOfLeadingZeros(value - 1));
  }

  public ONewLockManager() {
    for (int i = 0; i < locks.length; i++)
      locks[i] = new ReentrantReadWriteLock();
  }

  public Lock acquireExclusiveLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();

    return lock;
  }

  public Lock acquireExclusiveLock(int value) {
    final int index = index(value);

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();
    return lock;
  }

  public Lock acquireExclusiveLock(T value) {
    final int index = index(value.hashCode());

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.lock();
    return lock;
  }

  public Lock acquireSharedLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();

    return lock;

  }

  public Lock acquireSharedLock(int value) {
    final int index = index(value);

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();
    return lock;
  }

  public Lock acquireSharedLock(T value) {
    final int index = index(value.hashCode());

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.lock();
    return lock;
  }

  public void releaseSharedLock(int value) {
    final int index = index(value);

    final ReadWriteLock rwLock = locks[index];
    rwLock.readLock().unlock();
  }

  public void releaseSharedLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.unlock();
  }

  public void releaseSharedLock(T value) {
    final int index = index(value.hashCode());

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.readLock();
    lock.unlock();
  }

  public void releaseExclusiveLock(int value) {
    final int index = index(value);

    final ReadWriteLock rwLock = locks[index];
    rwLock.writeLock().unlock();
  }

  public void releaseExclusiveLock(long value) {
    final int hashCode = longHashCode(value);
    final int index = index(hashCode);

    final ReadWriteLock rwLock = locks[index];

    final Lock lock = rwLock.writeLock();
    lock.unlock();
  }

  public void releaseExclusiveLock(T value) {
    final int index = index(value.hashCode());

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
}