package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.sizemap;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

public final class EntryPoint extends ODurablePage {

  private static final int SIZE_OFFSET = NEXT_FREE_POSITION;
  private static final int FREE_LIST_HEADER_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;

  public EntryPoint(OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  public void setFileSize(int fileSize) {
    setIntValue(SIZE_OFFSET, fileSize);
  }

  public int getFileSize() {
    return getIntValue(SIZE_OFFSET);
  }

  public int getFreeListHeader() {
    return getIntValue(FREE_LIST_HEADER_OFFSET);
  }

  public void setFreeListHeader(int freeListHeader) {
    setIntValue(FREE_LIST_HEADER_OFFSET, freeListHeader);
  }
}
