package com.orientechnologies.orient.core.storage.cache;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by tglman on 23/06/16.
 */
public final class OCacheEntry {
  private       OCachePointer dataPointer;
  private final long          fileId;
  private final long          pageIndex;

  private final AtomicInteger usagesCount = new AtomicInteger();

  public OCacheEntry(final long fileId, final long pageIndex, final OCachePointer dataPointer) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
  }

  public OCachePointer getCachePointer() {
    return dataPointer;
  }

  public void clearCachePointer() {
    dataPointer = null;
  }

  public void setCachePointer(final OCachePointer cachePointer) {
    this.dataPointer = cachePointer;
  }

  public long getFileId() {
    return fileId;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public void setDirty() {
    dataPointer.setDirty();
  }

  public boolean isDirty() {
    return dataPointer.isDirty();
  }

  public void acquireExclusiveLock() {
    dataPointer.acquireExclusiveLock();
  }

  public void releaseExclusiveLock() {
    dataPointer.releaseExclusiveLock();
  }

  public void acquireSharedLock() {
    dataPointer.acquireSharedLock();
  }

  public void releaseSharedLock() {
    dataPointer.releaseSharedLock();
  }

  public int getUsagesCount() {
    return usagesCount.get();
  }

  public void incrementUsages() {
    usagesCount.incrementAndGet();
  }

  /**
   * DEBUG only !!
   *
   * @return Whether lock acquired on current entry
   */
  public boolean isLockAcquiredByCurrentThread() {
    return dataPointer.isLockAcquiredByCurrentThread();
  }

  public void decrementUsages() {
    usagesCount.decrementAndGet();
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    final OCacheEntry that = (OCacheEntry) o;

    if (fileId != that.fileId)
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
    result = 31 * result + usagesCount.get();
    return result;
  }

  @Override
  public String toString() {
    return "OReadCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", dataPointer=" + dataPointer + ", usagesCount="
        + usagesCount + '}';
  }

}
