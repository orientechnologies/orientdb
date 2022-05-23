package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.cache.chm.LRUList;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicIntegerFieldUpdater;

public class OCacheEntryImpl implements OCacheEntry {
  private static final AtomicIntegerFieldUpdater<OCacheEntryImpl> USAGES_COUNT_UPDATER;
  private static final AtomicIntegerFieldUpdater<OCacheEntryImpl> STATE_UPDATER;

  static {
    USAGES_COUNT_UPDATER =
        AtomicIntegerFieldUpdater.newUpdater(OCacheEntryImpl.class, "usagesCount");
    STATE_UPDATER = AtomicIntegerFieldUpdater.newUpdater(OCacheEntryImpl.class, "state");
  }

  private static final int FROZEN = -1;
  private static final int DEAD = -2;

  private OCachePointer dataPointer;
  private final long fileId;
  private final int pageIndex;

  private volatile int usagesCount;
  private volatile int state;

  private OCacheEntry next;
  private OCacheEntry prev;

  private LRUList container;

  /** Protected by page lock inside disk cache */
  private boolean allocatedPage;

  private int hash;
  private final boolean insideCache;
  private final OReadCache readCache;

  public OCacheEntryImpl(
      final long fileId,
      final int pageIndex,
      final OCachePointer dataPointer,
      final boolean insideCache,
      OReadCache readCache) {

    if (fileId < 0) {
      throw new IllegalStateException("File id has invalid value " + fileId);
    }

    if (pageIndex < 0) {
      throw new IllegalStateException("Page index has invalid value " + pageIndex);
    }

    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
    this.insideCache = insideCache;
    this.readCache = readCache;
  }

  public boolean isNewlyAllocatedPage() {
    return allocatedPage;
  }

  public void markAllocated() {
    allocatedPage = true;
  }

  public void clearAllocationFlag() {
    allocatedPage = false;
  }

  @Override
  public OCachePointer getCachePointer() {
    return dataPointer;
  }

  @Override
  public void clearCachePointer() {
    dataPointer = null;
  }

  @Override
  public void setCachePointer(final OCachePointer cachePointer) {
    this.dataPointer = cachePointer;
  }

  @Override
  public long getFileId() {
    return fileId;
  }

  @Override
  public int getPageIndex() {
    return pageIndex;
  }

  @Override
  public void acquireExclusiveLock() {
    dataPointer.acquireExclusiveLock();
  }

  @Override
  public void releaseExclusiveLock() {
    dataPointer.releaseExclusiveLock();
  }

  @Override
  public void acquireSharedLock() {
    dataPointer.acquireSharedLock();
  }

  @Override
  public void releaseSharedLock() {
    dataPointer.releaseSharedLock();
  }

  @Override
  public int getUsagesCount() {
    return USAGES_COUNT_UPDATER.get(this);
  }

  @Override
  public void incrementUsages() {
    USAGES_COUNT_UPDATER.incrementAndGet(this);
  }

  /**
   * DEBUG only !!
   *
   * @return Whether lock acquired on current entry
   */
  @Override
  public boolean isLockAcquiredByCurrentThread() {
    return dataPointer.isLockAcquiredByCurrentThread();
  }

  @Override
  public void decrementUsages() {
    USAGES_COUNT_UPDATER.decrementAndGet(this);
  }

  @Override
  public OWALChanges getChanges() {
    return null;
  }

  @Override
  public OLogSequenceNumber getInitialLSN() {
    return null;
  }

  @Override
  public void setInitialLSN(OLogSequenceNumber lsn) {}

  @Override
  public OLogSequenceNumber getEndLSN() {
    return dataPointer.getEndLSN();
  }

  @Override
  public void setEndLSN(final OLogSequenceNumber endLSN) {
    dataPointer.setEndLSN(endLSN);
  }

  @Override
  public boolean acquireEntry() {
    int state = STATE_UPDATER.get(this);

    while (state >= 0) {
      if (STATE_UPDATER.compareAndSet(this, state, state + 1)) {
        return true;
      }

      state = STATE_UPDATER.get(this);
    }

    return false;
  }

  @Override
  public void releaseEntry() {
    int state = STATE_UPDATER.get(this);

    while (true) {
      if (state <= 0) {
        throw new IllegalStateException(
            "Cache entry " + fileId + ":" + pageIndex + " has invalid state " + state);
      }

      if (STATE_UPDATER.compareAndSet(this, state, state - 1)) {
        return;
      }

      state = STATE_UPDATER.get(this);
    }
  }

  @Override
  public boolean isReleased() {
    return STATE_UPDATER.get(this) == 0;
  }

  @Override
  public boolean isAlive() {
    return STATE_UPDATER.get(this) >= 0;
  }

  @Override
  public boolean freeze() {
    int state = STATE_UPDATER.get(this);
    while (state == 0) {
      if (STATE_UPDATER.compareAndSet(this, state, FROZEN)) {
        return true;
      }

      state = STATE_UPDATER.get(this);
    }

    return false;
  }

  @Override
  public boolean isFrozen() {
    return STATE_UPDATER.get(this) == FROZEN;
  }

  @Override
  public void makeDead() {
    int state = STATE_UPDATER.get(this);

    while (state == FROZEN) {
      if (STATE_UPDATER.compareAndSet(this, state, DEAD)) {
        return;
      }

      state = STATE_UPDATER.get(this);
    }

    throw new IllegalStateException(
        "Cache entry " + fileId + ":" + pageIndex + " has invalid state " + state);
  }

  @Override
  public boolean isDead() {
    return STATE_UPDATER.get(this) == DEAD;
  }

  @Override
  public OCacheEntry getNext() {
    return next;
  }

  @Override
  public OCacheEntry getPrev() {
    return prev;
  }

  @Override
  public void setPrev(final OCacheEntry prev) {
    this.prev = prev;
  }

  @Override
  public void setNext(final OCacheEntry next) {
    this.next = next;
  }

  @Override
  public void setContainer(final LRUList lruList) {
    this.container = lruList;
  }

  @Override
  public LRUList getContainer() {
    return container;
  }

  @Override
  public boolean insideCache() {
    return insideCache;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OCacheEntryImpl that = (OCacheEntryImpl) o;

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
    return "OCacheEntryImpl{"
        + "dataPointer="
        + dataPointer
        + ", fileId="
        + fileId
        + ", pageIndex="
        + pageIndex
        + ", usagesCount="
        + usagesCount
        + '}';
  }

  @Override
  public void close() throws IOException {
    this.readCache.releaseFromRead(this);
  }
}
