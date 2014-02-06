package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import java.util.Iterator;

public class OrientVertexIterator extends OLazyWrapperIterator<Vertex> {
  private final OrientVertex             vertex;
  private final String[]                 iLabels;
  private final OPair<Direction, String> connection;

  public OrientVertexIterator(final OrientVertex orientVertex, final Iterator<?> iterator,
      final OPair<Direction, String> connection, final String[] iLabels, final int iSize) {
    super(iterator, iSize);
    this.vertex = orientVertex;
    this.connection = connection;
    this.iLabels = iLabels;
  }

  @Override
  public Vertex createWrapper(final Object iObject) {
    if (iObject instanceof OrientVertex)
      return (OrientVertex) iObject;

    final ORecord<?> rec = ((OIdentifiable) iObject).getRecord();

    if (rec == null || !(rec instanceof ODocument))
      return null;

    final ODocument value = (ODocument) rec;

    final OrientVertex v;
    if (value.getSchemaClass().isSubClassOf(OrientVertexType.CLASS_NAME)) {
      // DIRECT VERTEX
      v = new OrientVertex(vertex.graph, value);
    } else if (value.getSchemaClass().isSubClassOf(OrientEdgeType.CLASS_NAME)) {
      // EDGE
      if (vertex.settings.useVertexFieldsForEdgeLabels || OrientEdge.isLabeled(OrientEdge.getRecordLabel(value), iLabels))
        v = new OrientVertex(vertex.graph, OrientEdge.getConnection(value, connection.getKey().opposite()));
      else
        v = null;
    } else
      throw new IllegalStateException("Invalid content found between connections:" + value);

    return v;
  }

  public boolean filter(final Vertex iObject) {
    return true;
  }
}
