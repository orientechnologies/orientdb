package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Vertex;
import java.util.Iterator;

public class OrientClassVertexIterable extends OrientElementIterable<Vertex> {

  private String klass;

  public OrientClassVertexIterable(OrientBaseGraph graph, Iterable<?> iterable, String klass) {
    super(graph, iterable);
    this.klass = klass;
  }

  @Override
  public Iterator<Vertex> iterator() {
    return new OrientClassVertexIterator(super.graph, super.iterator(), klass);
  }
}
