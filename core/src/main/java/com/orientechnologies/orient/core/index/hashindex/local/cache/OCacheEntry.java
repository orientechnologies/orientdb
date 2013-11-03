package com.orientechnologies.orient.core.index.hashindex.local.cache;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OCacheEntry {
  OCachePointer dataPointer;

  final long    fileId;
  final long    pageIndex;

  boolean       isDirty;
  int           usagesCount;

  public OCacheEntry(long fileId, long pageIndex, OCachePointer dataPointer, boolean dirty) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;

    this.dataPointer = dataPointer;
    isDirty = dirty;
  }

  public void markDirty() {
    this.isDirty = true;
  }

  public OCachePointer getCachePointer() {
    return dataPointer;
  }

  public long getFileId() {
    return fileId;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OCacheEntry that = (OCacheEntry) o;

    if (fileId != that.fileId)
      return false;
    if (isDirty != that.isDirty)
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
    result = 31 * result + (isDirty ? 1 : 0);
    result = 31 * result + usagesCount;
    return result;
  }

  @Override
  public String toString() {
    return "OReadCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", dataPointer=" + dataPointer + ", isDirty="
        + isDirty + ", usagesCount=" + usagesCount + '}';
  }
}
