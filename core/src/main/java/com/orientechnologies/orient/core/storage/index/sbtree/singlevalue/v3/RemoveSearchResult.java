package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import java.util.List;

final class RemoveSearchResult {
  private final long leafPageIndex;
  private final int leafEntryPageIndex;
  private final List<RemovalPathItem> path;

  public RemoveSearchResult(
      long leafPageIndex, int leafEntryPageIndex, List<RemovalPathItem> path) {
    this.leafPageIndex = leafPageIndex;
    this.leafEntryPageIndex = leafEntryPageIndex;
    this.path = path;
  }

  public long getLeafPageIndex() {
    return leafPageIndex;
  }

  public int getLeafEntryPageIndex() {
    return leafEntryPageIndex;
  }

  public List<RemovalPathItem> getPath() {
    return path;
  }
}
