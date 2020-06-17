package com.orientechnologies.orient.core.storage.impl.local;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;

public class OClusterBrowsePage implements Iterable<OClusterBrowseEntry> {
  private List<OClusterBrowseEntry> entries;
  private long lastPosition;

  public OClusterBrowsePage(List<OClusterBrowseEntry> entries, long lastPosition) {
    this.entries = entries;
    this.lastPosition = lastPosition;
  }

  @Override
  public Iterator<OClusterBrowseEntry> iterator() {
    return entries.iterator();
  }

  @Override
  public Spliterator<OClusterBrowseEntry> spliterator() {
    return entries.spliterator();
  }

  public long getLastPosition() {
    return lastPosition;
  }
}
