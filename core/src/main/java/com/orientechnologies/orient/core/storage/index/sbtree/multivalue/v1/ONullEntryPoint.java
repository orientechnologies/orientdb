package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v1;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;

final class ONullEntryPoint extends ODurablePage {
  private static final int SIZE_OFFSET             = NEXT_FREE_POSITION;
  private static final int FREE_LIST_HEADER_OFFSET = SIZE_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int FIRST_PAGE_OFFSET       = FREE_LIST_HEADER_OFFSET + OIntegerSerializer.INT_SIZE;
  private static final int LAST_PAGE_OFFSET        = FIRST_PAGE_OFFSET + OIntegerSerializer.INT_SIZE;

  ONullEntryPoint(final OCacheEntry cacheEntry) {
    super(cacheEntry);
  }

  void setSize(final int size) {
    setIntValue(SIZE_OFFSET, size);
  }

  int getSize() {
    return getIntValue(SIZE_OFFSET);
  }

  void setFreeListHeader(final int header) {
    setIntValue(FREE_LIST_HEADER_OFFSET, header);
  }

  int getFreeListHeader() {
    return getIntValue(FREE_LIST_HEADER_OFFSET);
  }

  void setFirsPage(final int page) {
    setIntValue(FIRST_PAGE_OFFSET, page);
  }

  int getFirstPage() {
    return getIntValue(FIRST_PAGE_OFFSET);
  }

  void setLastPage(final int page) {
    setIntValue(LAST_PAGE_OFFSET, page);
  }

  int getLastPage() {
    return getIntValue(LAST_PAGE_OFFSET);
  }
}
