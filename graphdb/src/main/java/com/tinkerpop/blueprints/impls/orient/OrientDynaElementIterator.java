package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
class OrientDynaElementIterator implements Iterator<Object> {

  private final Iterator<?>     itty;
  private final OrientBaseGraph graph;

  public OrientDynaElementIterator(final OrientBaseGraph graph, final Iterator<?> itty) {
    this.itty = itty;
    this.graph = graph;
  }

  public boolean hasNext() {
    return this.itty.hasNext();
  }

  public Object next() {
    OrientElement currentElement = null;

    if (!hasNext())
      throw new NoSuchElementException();

    Object current = itty.next();

    if (null == current)
      throw new NoSuchElementException();

    if (current instanceof OIdentifiable)
      current = ((OIdentifiable) current).getRecord();

    if (current instanceof ODocument) {
      final ODocument currentDocument = (ODocument) current;

      if (currentDocument.getInternalStatus() == ODocument.STATUS.NOT_LOADED)
        currentDocument.load();

      final OClass schemaClass = currentDocument.getSchemaClass();
      if (schemaClass != null && schemaClass.isSubClassOf(graph.getEdgeBaseType()))
        currentElement = new OrientEdge(graph, currentDocument);
      else
        // RETURN VERTEX IN ALL THE CASES, EVEN FOR PROJECTED DOCUMENTS
        currentElement = new OrientVertex(graph, currentDocument);
    }

    return currentElement;
  }

  public void remove() {
    throw new UnsupportedOperationException();
  }
}