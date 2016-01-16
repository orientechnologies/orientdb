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
package com.orientechnologies.orient.core.storage.cache;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;
import com.orientechnologies.common.directmemory.ODirectMemoryPointerFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 05.08.13
 */
public class OCachePointer {
  private static final int            WRITERS_OFFSET         = 32;
  private static final int            READERS_MASK           = 0xFFFFFFFF;

  private final ReadWriteLock         readWriteLock          = new ReentrantReadWriteLock();

  private final AtomicInteger         referrersCount         = new AtomicInteger();
  private final AtomicLong            readersWritersReferrer = new AtomicLong();

  private final AtomicInteger         usagesCounter          = new AtomicInteger();

  private volatile OLogSequenceNumber lastFlushedLsn;

  private volatile WritersListener    writersListener;

  private final ODirectMemoryPointer  dataPointer;
  private final long                  fileId;
  private final long                  pageIndex;

  public OCachePointer(ODirectMemoryPointer dataPointer, OLogSequenceNumber lastFlushedLsn, long fileId, long pageIndex) {
    this.lastFlushedLsn = lastFlushedLsn;
    this.dataPointer = dataPointer;

    this.fileId = fileId;
    this.pageIndex = pageIndex;
  }

  public OCachePointer(byte[] data, OLogSequenceNumber lastFlushedLsn, long fileId, long pageIndex) {
    this.lastFlushedLsn = lastFlushedLsn;
    dataPointer = ODirectMemoryPointerFactory.instance().createPointer(data);

    this.fileId = fileId;
    this.pageIndex = pageIndex;
  }

  public void setWritersListener(WritersListener writersListener) {
    this.writersListener = writersListener;
  }

  public long getFileId() {
    return fileId;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public OLogSequenceNumber getLastFlushedLsn() {
    return lastFlushedLsn;
  }

  public void setLastFlushedLsn(OLogSequenceNumber lastFlushedLsn) {
    this.lastFlushedLsn = lastFlushedLsn;
  }

  public void incrementReadersReferrer() {
    long readersWriters = readersWritersReferrer.get();
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    readers++;

    while (!readersWritersReferrer.compareAndSet(readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = readersWritersReferrer.get();
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      readers++;
    }

    final WritersListener wl = writersListener;
    if (wl != null) {
      if (writers > 0 && readers == 1)
        wl.removeOnlyWriters(fileId, pageIndex);
    }

    incrementReferrer();
  }

  public void decrementReadersReferrer() {
    long readersWriters = readersWritersReferrer.get();
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    readers--;

    assert readers >= 0;

    while (!readersWritersReferrer.compareAndSet(readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = readersWritersReferrer.get();
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      readers--;

      assert readers >= 0;
    }

    final WritersListener wl = writersListener;
    if (wl != null) {
      if (writers > 0 && readers == 0)
        wl.addOnlyWriters(fileId, pageIndex);
    }

    decrementReferrer();
  }

  public void incrementWritersReferrer() {
    long readersWriters = readersWritersReferrer.get();
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    writers++;

    while (!readersWritersReferrer.compareAndSet(readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = readersWritersReferrer.get();
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      writers++;
    }

    incrementReferrer();
  }

  public void decrementWritersReferrer() {
    long readersWriters = readersWritersReferrer.get();
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    writers--;

    assert writers >= 0;

    while (!readersWritersReferrer.compareAndSet(readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = readersWritersReferrer.get();
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      writers--;

      assert writers >= 0;
    }

    final WritersListener wl = writersListener;
    if (wl != null) {
      if (readers == 0 && writers == 0)
        wl.removeOnlyWriters(fileId, pageIndex);
    }

    decrementReferrer();
  }

  public void incrementReferrer() {
    referrersCount.incrementAndGet();
  }

  public void decrementReferrer() {
    final int rf = referrersCount.decrementAndGet();
    if (rf == 0) {
      dataPointer.free();
    }

    if (rf < 0)
      throw new IllegalStateException("Invalid direct memory state, number of referrers can not be negative " + rf);
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

  private long composeReadersWriters(int readers, int writers) {
    return ((long) writers) << WRITERS_OFFSET | readers;
  }

  private int getReaders(long readersWriters) {
    return (int) (readersWriters & READERS_MASK);
  }

  private int getWriters(long readersWriters) {
    return (int) (readersWriters >>> WRITERS_OFFSET);
  }

  public interface WritersListener {
    void addOnlyWriters(long fileId, long pageIndex);

    void removeOnlyWriters(long fileId, long pageIndex);
  }

}
