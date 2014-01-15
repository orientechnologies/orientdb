package com.tinkerpop.blueprints.impls.orient;

import java.util.Collections;
import java.util.Iterator;

import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;

/**
 * @author Marko A. Rodriguez (http://markorodriguez.com)
 */
public class OrientElementIterable<T extends Element> implements CloseableIterable<T> {

  private final Iterable<?>     iterable;
  private final OrientBaseGraph graph;

  public OrientElementIterable(final OrientBaseGraph graph, final Iterable<?> iterable) {
    this.graph = graph;
    this.iterable = iterable;
  }

  @SuppressWarnings("unchecked")
  public Iterator<T> iterator() {
    if (iterable == null)
      return Collections.EMPTY_LIST.iterator();

    return new OrientElementIterator<T>(this.graph, iterable.iterator());
  }

  public void close() {

  }

}
