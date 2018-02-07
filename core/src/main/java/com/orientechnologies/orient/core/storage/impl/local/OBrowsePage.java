package com.orientechnologies.orient.core.storage.impl.local;

import com.orientechnologies.orient.core.storage.ORawBuffer;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;

public class OBrowsePage implements Iterable<OBrowsePage.OBrowseEntry> {
  public static class OBrowseEntry {
    private long       clusterPosition;
    private ORawBuffer buffer;

    public OBrowseEntry(long clusterPosition, ORawBuffer buffer) {
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

  private List<OBrowseEntry> entries;
  private long               lastPosition;

  public OBrowsePage(List<OBrowseEntry> entries, long lastPosition) {
    this.entries = entries;
    this.lastPosition = lastPosition;
  }

  @Override
  public Iterator<OBrowseEntry> iterator() {
    return entries.iterator();
  }

  @Override
  public Spliterator<OBrowseEntry> spliterator() {
    return entries.spliterator();
  }

  public long getLastPosition() {
    return lastPosition;
  }
}
