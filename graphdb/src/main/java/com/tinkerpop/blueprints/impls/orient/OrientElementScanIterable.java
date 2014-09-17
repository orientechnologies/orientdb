package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordAbstract;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.tinkerpop.blueprints.CloseableIterable;
import com.tinkerpop.blueprints.Element;

import java.util.Iterator;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
class OrientElementScanIterable<T extends Element> implements CloseableIterable<T> {
  private final String          elementClass;
  private final OrientBaseGraph graph;
  private final boolean         polymorphic;

  public OrientElementScanIterable(final OrientBaseGraph graph, final String elementClass, final boolean polymorphic) {
    this.graph = graph;
    this.elementClass = elementClass;
    this.polymorphic = polymorphic;
  }

  public Iterator<T> iterator() {
    final ODatabaseDocumentTx rawGraph = this.graph.getRawGraph();
    return new OrientElementIterator<T>(this.graph, new ORecordIteratorClass<ORecordInternal>(rawGraph,
        (ODatabaseRecordAbstract) rawGraph.getUnderlying(), elementClass, polymorphic));
  }

  public void close() {
  }
}
