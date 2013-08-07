package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
class OCacheEntry {
  final long         fileId;
  final long         pageIndex;

  OLogSequenceNumber loadedLSN;

  OCachePointer      dataPointer;

  boolean            isDirty;

  public OCacheEntry(long fileId, long pageIndex, OCachePointer dataPointer, boolean dirty, OLogSequenceNumber loadedLSN) {
    this.fileId = fileId;
    this.pageIndex = pageIndex;
    this.loadedLSN = loadedLSN;
    this.dataPointer = dataPointer;
    isDirty = dirty;
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
    if (dataPointer != null ? !dataPointer.equals(that.dataPointer) : that.dataPointer != null)
      return false;
    if (loadedLSN != null ? !loadedLSN.equals(that.loadedLSN) : that.loadedLSN != null)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (loadedLSN != null ? loadedLSN.hashCode() : 0);
    result = 31 * result + (dataPointer != null ? dataPointer.hashCode() : 0);
    result = 31 * result + (isDirty ? 1 : 0);
    return result;
  }

  @Override
  public String toString() {
    return "OCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", loadedLSN=" + loadedLSN + ", dataPointer="
        + dataPointer + ", isDirty=" + isDirty + '}';
  }
}
