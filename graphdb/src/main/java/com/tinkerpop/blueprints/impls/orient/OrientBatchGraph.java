package com.tinkerpop.blueprints.impls.orient;

import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.util.wrappers.batch.BatchGraph;
import com.tinkerpop.blueprints.util.wrappers.batch.VertexIDType;

/**
 * A Blueprints implementation of the script graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientBatchGraph extends BatchGraph<OrientTransactionalGraph> {

  public OrientBatchGraph(final OrientTransactionalGraph graph) {
    super(graph);
  }

  public OrientBatchGraph(final OrientTransactionalGraph graph, final VertexIDType type, final long bufferSize) {
    super(graph, type, bufferSize);
  }

  protected <E extends Element> E setProperties(final E element, final Object... properties) {
    ((OrientElement) element).setProperties(properties);
    if (!((OrientElement) element).isDetached())
      ((OrientElement) element).save();
    return element;
  }
}
