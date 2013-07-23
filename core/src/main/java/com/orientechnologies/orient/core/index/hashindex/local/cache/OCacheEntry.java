package com.orientechnologies.orient.core.index.hashindex.local.cache;

import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;

/**
 * @author Andrey Lomakin
 * @since 7/23/13
 */
public class OCacheEntry {
  long               fileId;
  long               pageIndex;

  OLogSequenceNumber loadedLSN;

  long               dataPointer;

  boolean            isDirty;
  int                usageCounter;

  public OCacheEntry(long fileId, long pageIndex, long dataPointer, boolean dirty, OLogSequenceNumber loadedLSN) {
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

    if (dataPointer != that.dataPointer)
      return false;
    if (fileId != that.fileId)
      return false;
    if (pageIndex != that.pageIndex)
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
    result = 31 * result + (int) (dataPointer ^ (dataPointer >>> 32));
    return result;
  }

  @Override
  public String toString() {
    return "OCacheEntry{" + "fileId=" + fileId + ", pageIndex=" + pageIndex + ", loadedLSN=" + loadedLSN + ", dataPointer="
        + dataPointer + ", isDirty=" + isDirty + ", usageCounter=" + usageCounter + '}';
  }
}
