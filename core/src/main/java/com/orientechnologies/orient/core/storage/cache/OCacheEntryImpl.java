package com.orientechnologies.orient.core.storage.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALChanges;

/**
 * Created by tglman on 23/06/16.
 */
public class OCacheEntryImpl implements OCacheEntry{
  OCachePointer dataPointer;
  final long    fileId;
  final long    pageIndex;

  boolean       dirty;
  int           usagesCount;

  public OCacheEntryImpl(long fileId, long pageIndex, OCachePointer dataPointer, boolean dirty) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
    this.dirty = dirty;
  }

  public void markDirty() {
    this.dirty = true;
  }

  public void clearDirty() {
    this.dirty = false;
  }

  public boolean isDirty() {
    return dirty;
  }

  public OCachePointer getCachePointer() {
    return dataPointer;
  }

  public void clearCachePointer() {
    dataPointer = null;
  }

  public void setCachePointer(OCachePointer cachePointer) {
    this.dataPointer = cachePointer;
  }

  public long getFileId() {
    return fileId;
  }

  public long getPageIndex() {
    return pageIndex;
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
    return usagesCount;
  }

  public void incrementUsages() {
    usagesCount++;
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
    usagesCount--;
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
    if (usagesCount != that.usagesCount)
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
    result = 31 * result + usagesCount;
    return result;
  }

  @Override
  public String toString() {
    return "OReadCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", dataPointer=" + dataPointer + ", dirty="
        + dirty + ", usagesCount=" + usagesCount + '}';
  }

}
