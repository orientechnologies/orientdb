package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Iterator;

/** Created by luigidellaquila on 02/07/16. */
public class OEdgeToVertexIterator implements Iterator<OVertex> {
  private final Iterator<OEdge> edgeIterator;
  private final ODirection direction;

  public OEdgeToVertexIterator(Iterator<OEdge> iterator, ODirection direction) {
    if (direction == ODirection.BOTH) {
      throw new IllegalArgumentException(
          "edge to vertex iterator does not support BOTH as direction");
    }
    this.edgeIterator = iterator;
    this.direction = direction;
  }

  @Override
  public boolean hasNext() {
    return edgeIterator.hasNext();
  }

  @Override
  public OVertex next() {
    OEdge edge = edgeIterator.next();
    switch (direction) {
      case OUT:
        return edge.getTo();
      case IN:
        return edge.getFrom();
    }
    return null;
  }
}
