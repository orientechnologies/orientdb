package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

final class BucketSearchResult {
  private final int itemIndex;
  private final long pageIndex;

  public BucketSearchResult(final int itemIndex, final long pageIndex) {
    this.itemIndex = itemIndex;
    this.pageIndex = pageIndex;
  }

  public int getItemIndex() {
    return itemIndex;
  }

  public long getPageIndex() {
    return pageIndex;
  }
}
