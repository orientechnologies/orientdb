package com.orientechnologies.orient.core.storage.index.sbtreebonsai.global.btree;

import java.util.ArrayList;
import java.util.List;

public final class UpdateBucketSearchResult {

  private final List<Integer> insertionIndexes;
  private final ArrayList<Integer> path;
  private final int itemIndex;

  public UpdateBucketSearchResult(
      final List<Integer> insertionIndexes, final ArrayList<Integer> path, final int itemIndex) {
    this.insertionIndexes = insertionIndexes;
    this.path = path;
    this.itemIndex = itemIndex;
  }

  public long getLastPathItem() {
    return getPath().get(getPath().size() - 1);
  }

  public List<Integer> getInsertionIndexes() {
    return insertionIndexes;
  }

  public ArrayList<Integer> getPath() {
    return path;
  }

  public int getItemIndex() {
    return itemIndex;
  }
}
