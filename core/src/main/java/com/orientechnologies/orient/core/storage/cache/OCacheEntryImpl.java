package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tglman on 23/06/16.
 */
public class OCacheEntryImpl implements OCacheEntry {
  private       OCachePointer dataPointer;
  private final long          fileId;
  private final long          pageIndex;

  private boolean dirty;
  private final AtomicInteger usagesCount = new AtomicInteger();

  public OCacheEntryImpl(long fileId, long pageIndex, OCachePointer dataPointer, boolean dirty) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
    this.dirty = dirty;
  }

  @Override
  public void markDirty() {
    this.dirty = true;
  }

  @Override
  public void clearDirty() {
    this.dirty = false;
  }

  @Override
  public boolean isDirty() {
    return dirty;
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
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OCacheEntryImpl that = (OCacheEntryImpl) o;

    if (fileId != that.fileId)
      return false;
    if (dirty != that.dirty)
      return false;
    if (pageIndex != that.pageIndex)
      return false;
    if (usagesCount.get() != that.usagesCount.get())
      return false;
    if (dataPointer != null ? !dataPointer.equals(that.dataPointer) : that.dataPointer != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (dataPointer != null ? dataPointer.hashCode() : 0);
    result = 31 * result + (dirty ? 1 : 0);
    result = 31 * result + usagesCount.get();
    return result;
  }

  @Override
  public String toString() {
    return "OReadCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", dataPointer=" + dataPointer + ", dirty="
        + dirty + ", usagesCount=" + usagesCount + '}';
  }

}
