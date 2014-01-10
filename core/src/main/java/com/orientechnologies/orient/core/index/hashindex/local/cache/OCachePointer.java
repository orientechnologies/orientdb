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
package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 05.08.13
 */
public class OCachePointer {
  private final ReadWriteLock         readWriteLock  = new ReentrantReadWriteLock();

  private final AtomicInteger         referrersCount = new AtomicInteger();
  private final AtomicInteger         usagesCounter  = new AtomicInteger();

  private volatile OLogSequenceNumber lastFlushedLsn;

  private final ODirectMemoryPointer  dataPointer;

  public OCachePointer(ODirectMemoryPointer dataPointer, OLogSequenceNumber lastFlushedLsn) {
    this.lastFlushedLsn = lastFlushedLsn;
    this.dataPointer = dataPointer;
  }

  public OCachePointer(byte[] data, OLogSequenceNumber lastFlushedLsn) {
    this.lastFlushedLsn = lastFlushedLsn;
    dataPointer = new ODirectMemoryPointer(data);
  }

  OLogSequenceNumber getLastFlushedLsn() {
    return lastFlushedLsn;
  }

  void setLastFlushedLsn(OLogSequenceNumber lastFlushedLsn) {
    this.lastFlushedLsn = lastFlushedLsn;
  }

  void incrementReferrer() {
    referrersCount.incrementAndGet();
  }

  void decrementReferrer() {
    if (referrersCount.decrementAndGet() == 0) {
      dataPointer.free();
    }
  }

  public ODirectMemoryPointer getDataPointer() {
    return dataPointer;
  }

  public void acquireExclusiveLock() {
    readWriteLock.writeLock().lock();
  }

  public boolean tryAcquireExclusiveLock() {
    return readWriteLock.writeLock().tryLock();
  }

  public void releaseExclusiveLock() {
    readWriteLock.writeLock().unlock();
  }

  public void acquireSharedLock() {
    readWriteLock.readLock().lock();
  }

  public void releaseSharedLock() {
    readWriteLock.readLock().unlock();
  }

  public boolean tryAcquireSharedLock() {
    return readWriteLock.readLock().tryLock();
  }

  @Override
  protected void finalize() throws Throwable {
    super.finalize();

    if (referrersCount.get() > 0)
      dataPointer.free();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OCachePointer that = (OCachePointer) o;

    if (dataPointer != null ? !dataPointer.equals(that.dataPointer) : that.dataPointer != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    return dataPointer != null ? dataPointer.hashCode() : 0;
  }

  @Override
  public String toString() {
    return "OCachePointer{" + "referrersCount=" + referrersCount + ", usagesCount=" + usagesCounter + ", dataPointer="
        + dataPointer + '}';
  }

}
