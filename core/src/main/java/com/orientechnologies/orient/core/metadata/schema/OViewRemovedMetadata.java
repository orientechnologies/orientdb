package com.orientechnologies.orient.core.metadata.schema;

import java.util.List;

public class OViewRemovedMetadata {
  int[] clusters;
  List<String> indexes;

  public OViewRemovedMetadata(int[] clusters, List<String> oldIndexes) {
    this.clusters = clusters;
    this.indexes = oldIndexes;
  }

  public int[] getClusters() {
    return clusters;
  }

  public List<String> getIndexes() {
    return indexes;
  }
}
