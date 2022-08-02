package com.orientechnologies.orient.core.storage.index.sbtree.singlevalue.v3;

import java.util.ArrayList;
import java.util.List;

final class UpdateBucketSearchResult {
  private final List<Integer> insertionIndexes;
  private final ArrayList<Long> path;
  private final int itemIndex;

  public UpdateBucketSearchResult(
      final List<Integer> insertionIndexes, final ArrayList<Long> path, final int itemIndex) {
    this.insertionIndexes = insertionIndexes;
    this.path = path;
    this.itemIndex = itemIndex;
  }

  public long getLastPathItem() {
    return getPath().get(getPath().size() - 1);
  }

  public ArrayList<Long> getPath() {
    return path;
  }

  public List<Integer> getInsertionIndexes() {
    return insertionIndexes;
  }

  public int getItemIndex() {
    return itemIndex;
  }
}
