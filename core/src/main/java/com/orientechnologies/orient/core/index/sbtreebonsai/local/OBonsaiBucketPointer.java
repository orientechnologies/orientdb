package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;

/**
 * @author <a href="mailto:enisher@gmail.com">Artem Orobets</a>
 */
public class OBonsaiBucketPointer {
  public static final int                  SIZE = OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE;
  public static final OBonsaiBucketPointer NULL = new OBonsaiBucketPointer(-1, -1);

  private final long                       pageIndex;
  private final int                        pageOffset;

  public OBonsaiBucketPointer(long pageIndex, int pageOffset) {
    this.pageIndex = pageIndex;
    this.pageOffset = pageOffset;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public int getPageOffset() {
    return pageOffset;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    OBonsaiBucketPointer that = (OBonsaiBucketPointer) o;

    if (pageIndex != that.pageIndex)
      return false;
    if (pageOffset != that.pageOffset)
      return false;

    return true;
  }

  @Override
  public int hashCode() {
    int result = (int) (pageIndex ^ (pageIndex >>> 32));
    result = 31 * result + pageOffset;
    return result;
  }

  public boolean isValid() {
    return pageIndex >= 0;
  }
}
