package com.tinkerpop.blueprints.impls.orient;

import java.util.Iterator;
import java.util.NoSuchElementException;

import com.tinkerpop.blueprints.Vertex;

public class OrientClassVertexIterator implements Iterator<Vertex> {
  private Iterator<Vertex> iterator;
  private String           klass;
  private OrientVertex     vertex;

  public OrientClassVertexIterator(Iterator<Vertex> iterator, String klass) {
    this.iterator = iterator;
    this.klass = klass;
  }

  @Override
  public boolean hasNext() {
    while (iterator.hasNext()) {
      vertex = (OrientVertex) iterator.next();
      if (vertex != null && klass.equals(vertex.getLabel())) {
        return true;
      }
    }
    return false;
  }

  @Override
  public Vertex next() {
    if (vertex == null) {
      if (!hasNext())
        throw new NoSuchElementException();
    }

    OrientVertex cur = vertex;
    vertex = null;
    return cur;
  }

  @Override
  public void remove() {
    iterator.remove();
  }

}
