package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.storage.ORawBuffer;

public class OClusterBrowseEntry {
  private long clusterPosition;
  private ORawBuffer buffer;

  public OClusterBrowseEntry(long clusterPosition, ORawBuffer buffer) {
    this.clusterPosition = clusterPosition;
    this.buffer = buffer;
  }

  public long getClusterPosition() {
    return clusterPosition;
  }

  public ORawBuffer getBuffer() {
    return buffer;
  }
}
