package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

final class RemovalPathItem {
  private final long pageIndex;
  private final int indexInsidePage;
  private final boolean leftChild;

  public RemovalPathItem(long pageIndex, int indexInsidePage, boolean leftChild) {
    this.pageIndex = pageIndex;
    this.indexInsidePage = indexInsidePage;
    this.leftChild = leftChild;
  }

  public long getPageIndex() {
    return pageIndex;
  }

  public int getIndexInsidePage() {
    return indexInsidePage;
  }

  public boolean isLeftChild() {
    return leftChild;
  }
}
