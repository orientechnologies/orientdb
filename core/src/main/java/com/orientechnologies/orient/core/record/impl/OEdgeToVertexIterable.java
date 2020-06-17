package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Collection;
import java.util.Iterator;

/** Created by luigidellaquila on 02/07/16. */
public class OEdgeToVertexIterable implements Iterable<OVertex>, OSizeable {
  private final Iterable<OEdge> edges;
  private final ODirection direction;

  public OEdgeToVertexIterable(Iterable<OEdge> edges, ODirection direction) {
    this.edges = edges;
    this.direction = direction;
  }

  @Override
  public Iterator<OVertex> iterator() {
    return new OEdgeToVertexIterator(edges.iterator(), direction);
  }

  @Override
  public int size() {
    if (edges == null) {
      return 0;
    }
    if (edges instanceof OSizeable) {
      return ((OSizeable) edges).size();
    }
    if (edges instanceof Collection) {
      return ((Collection) edges).size();
    }
    Iterator<OEdge> iterator = edges.iterator();
    int count = 0;
    while (iterator.hasNext()) {
      count++;
    }
    return count;
  }
}
