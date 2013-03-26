package com.orientechnologies.orient.core.index.hashindex.local.arc;

/**
 * @author Artem Loginov(logart)
 */
public class OCacheEntry {
  long           fileId;
  public long    pageIndex;

  public long    dataPointer;

  public boolean managedExternally;
  public boolean isDirty;

  public boolean isLoaded = true;

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
    if (isDirty != that.isDirty)
      return false;
    if (managedExternally != that.managedExternally)
      return false;
    if (pageIndex != that.pageIndex)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (fileId ^ (fileId >>> 32));
    result = 31 * result + (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + (int) (dataPointer ^ (dataPointer >>> 32));
    result = 31 * result + (managedExternally ? 1 : 0);
    result = 31 * result + (isDirty ? 1 : 0);
    return result;
  }
}
