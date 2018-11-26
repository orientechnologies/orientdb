package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tglman on 23/06/16.
 */
public class OCacheEntryImpl implements OCacheEntry {
  private       OCachePointer dataPointer;
  private final long          fileId;
  private final long          pageIndex;

  private final AtomicInteger usagesCount = new AtomicInteger();

  public OCacheEntryImpl(long fileId, long pageIndex, OCachePointer dataPointer) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
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
  public void setCachePointer(OCachePointer cachePointer) {
    this.dataPointer = cachePointer;
  }

  @Override
  public long getFileId() {
    return fileId;
  }

  @Override
  public long getPageIndex() {
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
    return usagesCount.get();
  }

  @Override
  public void incrementUsages() {
    usagesCount.incrementAndGet();
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
    usagesCount.decrementAndGet();
  }

  @Override
  public OWALChanges getChanges() {
    return null;
  }

  @Override
  public OLogSequenceNumber getEndLSN() {
    return dataPointer.getEndLSN();
  }

  @Override
  public void setEndLSN(OLogSequenceNumber endLSN) {
    dataPointer.setEndLSN(endLSN);
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
    return fileId == that.fileId && pageIndex == that.pageIndex;
  }

  @Override
  public int hashCode() {
    return Objects.hash(fileId, pageIndex);
  }

  @Override
  public String toString() {
    return "OCacheEntryImpl{" + "dataPointer=" + dataPointer + ", fileId=" + fileId + ", pageIndex=" + pageIndex + ", usagesCount="
        + usagesCount + '}';
  }
}
