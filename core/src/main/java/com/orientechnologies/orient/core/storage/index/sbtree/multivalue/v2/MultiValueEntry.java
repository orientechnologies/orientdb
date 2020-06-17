package com.orientechnologies.orient.core.storage.index.sbtree.multivalue.v2;

final class MultiValueEntry implements Comparable<MultiValueEntry> {
  public final long id;
  public final int clusterId;
  public final long clusterPosition;

  MultiValueEntry(final long id, final int clusterId, final long clusterPosition) {
    this.id = id;
    this.clusterId = clusterId;
    this.clusterPosition = clusterPosition;
  }

  @Override
  public int compareTo(final MultiValueEntry o) {
    int result = Long.compare(id, o.id);
    if (result != 0) {
      return result;
    }

    result = Integer.compare(clusterId, o.clusterId);
    if (result != 0) {
      return result;
    }

    return Long.compare(clusterPosition, o.clusterPosition);
  }
}
