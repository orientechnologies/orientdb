package com.tinkerpop.blueprints.impls.orient;

import java.util.Iterator;

import com.tinkerpop.blueprints.CloseableIterable;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientDynaElementIterable implements CloseableIterable<Object> {

  private final Iterator<?>     iterator;
  private final OrientBaseGraph graph;

  public OrientDynaElementIterable(final OrientBaseGraph graph, final Iterable<?> iterable) {
    this.graph = graph;
    this.iterator = iterable.iterator();
  }

  public OrientDynaElementIterable(final OrientBaseGraph graph, final Iterator<?> iterator) {
    this.graph = graph;
    this.iterator = iterator;
  }

  public Iterator<Object> iterator() {
    return new OrientDynaElementIterator(this.graph, iterator);
  }

  public void close() {

  }

}
