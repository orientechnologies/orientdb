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
package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;
import java.util.concurrent.atomic.AtomicLongFieldUpdater;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 05.08.13
 */
public final class OCachePointer {
  private static final AtomicIntegerFieldUpdater<OCachePointer> REFERRERS_COUNT_UPDATER;
  private static final AtomicLongFieldUpdater<OCachePointer> READERS_WRITERS_REFERRER_UPDATER;

  static {
    REFERRERS_COUNT_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(OCachePointer.class, "referrersCount");
    READERS_WRITERS_REFERRER_UPDATER =
        AtomicLongFieldUpdater.newUpdater(OCachePointer.class, "readersWritersReferrer");
  }

  private static final int WRITERS_OFFSET = 32;
  private static final int READERS_MASK = 0xFFFFFFFF;

  private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

  private volatile int referrersCount;

  private volatile long readersWritersReferrer;

  private volatile WritersListener writersListener;

  private final OPointer pointer;
  private final OByteBufferPool bufferPool;

  private long version;

  private final long fileId;
  private final int pageIndex;

  private OLogSequenceNumber endLSN;

  private int hash;

  public OCachePointer(
      final OPointer pointer,
      final OByteBufferPool bufferPool,
      final long fileId,
      final int pageIndex) {
    this.pointer = pointer;
    this.bufferPool = bufferPool;

    if (fileId < 0) {
      throw new IllegalStateException("File id has invalid value " + fileId);
    }

    if (pageIndex < 0) {
      throw new IllegalStateException("Page index has invalid value " + pageIndex);
    }

    this.fileId = fileId;
    this.pageIndex = pageIndex;
  }

  public void setWritersListener(WritersListener writersListener) {
    this.writersListener = writersListener;
  }

  public long getFileId() {
    return fileId;
  }

  public int getPageIndex() {
    return pageIndex;
  }

  public void incrementReadersReferrer() {
    long readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    readers++;

    while (!READERS_WRITERS_REFERRER_UPDATER.compareAndSet(
        this, readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      readers++;
    }

    final WritersListener wl = writersListener;
    if (wl != null) {
      if (writers > 0 && readers == 1) {
        wl.removeOnlyWriters(fileId, pageIndex);
      }
    }

    incrementReferrer();
  }

  public void decrementReadersReferrer() {
    long readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    readers--;

    assert readers >= 0;

    while (!READERS_WRITERS_REFERRER_UPDATER.compareAndSet(
        this, readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      readers--;

      assert readers >= 0;
    }

    final WritersListener wl = writersListener;
    if (wl != null) {
      if (writers > 0 && readers == 0) {
        wl.addOnlyWriters(fileId, pageIndex);
      }
    }

    decrementReferrer();
  }

  public void incrementWritersReferrer() {
    long readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    writers++;

    while (!READERS_WRITERS_REFERRER_UPDATER.compareAndSet(
        this, readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      writers++;
    }

    incrementReferrer();
  }

  public void decrementWritersReferrer() {
    long readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
    int readers = getReaders(readersWriters);
    int writers = getWriters(readersWriters);
    writers--;

    assert writers >= 0;

    while (!READERS_WRITERS_REFERRER_UPDATER.compareAndSet(
        this, readersWriters, composeReadersWriters(readers, writers))) {
      readersWriters = READERS_WRITERS_REFERRER_UPDATER.get(this);
      readers = getReaders(readersWriters);
      writers = getWriters(readersWriters);
      writers--;

      assert writers >= 0;
    }

    final WritersListener wl = writersListener;
    if (wl != null) {
      if (readers == 0 && writers == 0) {
        wl.removeOnlyWriters(fileId, pageIndex);
      }
    }

    decrementReferrer();
  }

  /**
   * DEBUG only !!!
   *
   * @return Whether pointer lock (read or write )is acquired
   */
  boolean isLockAcquiredByCurrentThread() {
    return readWriteLock.getReadHoldCount() > 0 || readWriteLock.isWriteLockedByCurrentThread();
  }

  public void incrementReferrer() {
    REFERRERS_COUNT_UPDATER.incrementAndGet(this);
  }

  public void decrementReferrer() {
    final int rf = REFERRERS_COUNT_UPDATER.decrementAndGet(this);
    if (rf == 0 && pointer != null) {
      bufferPool.release(pointer);
    }

    if (rf < 0) {
      throw new IllegalStateException(
          "Invalid direct memory state, number of referrers cannot be negative " + rf);
    }
  }

  public ByteBuffer getBuffer() {
    if (pointer == null) {
      return null;
    }

    return pointer.getNativeByteBuffer();
  }

  public OPointer getPointer() {
    return pointer;
  }

  public ByteBuffer getBufferDuplicate() {
    if (pointer == null) {
      return null;
    }
    final ByteBuffer duplicate =
        pointer.getNativeByteBuffer().duplicate().order(ByteOrder.nativeOrder());
    duplicate.rewind();
    return duplicate;
  }

  public void acquireExclusiveLock() {
    readWriteLock.writeLock().lock();
    version++;
  }

  public long getVersion() {
    return version;
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
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;

    OCachePointer that = (OCachePointer) o;

    if (fileId != that.fileId) {
      return false;
    }
    return pageIndex == that.pageIndex;
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }

    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + pageIndex;

    hash = result;

    return hash;
  }

  @Override
  public String toString() {
    return "OCachePointer{" + "referrersCount=" + referrersCount + '}';
  }

  private static long composeReadersWriters(int readers, int writers) {
    return ((long) writers) << WRITERS_OFFSET | readers;
  }

  private static int getReaders(long readersWriters) {
    return (int) (readersWriters & READERS_MASK);
  }

  private static int getWriters(long readersWriters) {
    return (int) (readersWriters >>> WRITERS_OFFSET);
  }

  public interface WritersListener {
    void addOnlyWriters(long fileId, long pageIndex);

    void removeOnlyWriters(long fileId, long pageIndex);
  }

  public OLogSequenceNumber getEndLSN() {
    return endLSN;
  }

  void setEndLSN(OLogSequenceNumber endLSN) {
    this.endLSN = endLSN;
  }
}
