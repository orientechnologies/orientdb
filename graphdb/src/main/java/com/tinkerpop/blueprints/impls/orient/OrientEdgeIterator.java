package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;

import java.util.Iterator;

/**
 * Lazy iterator of edges.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientEdgeIterator extends OLazyWrapperIterator<OrientEdge> {
  private final OrientVertex             sourceVertex;
  private final OrientVertex             targetVertex;
  private final OPair<Direction, String> connection;
  private final String[]                 labels;

  public OrientEdgeIterator(final OrientVertex iSourceVertex, final OrientVertex iTargetVertex, final Iterator<?> iterator,
      final OPair<Direction, String> connection, final String[] iLabels, final int iSize) {
    super(iterator, iSize);
    this.sourceVertex = iSourceVertex;
    this.targetVertex = iTargetVertex;
    this.connection = connection;
    this.labels = iLabels;
  }

  @Override
  public OrientEdge createWrapper(final Object iObject) {
    if (iObject instanceof OrientEdge)
      return (OrientEdge) iObject;

    final OIdentifiable rec = (OIdentifiable) iObject;

    final ORecord<?> record = rec.getRecord();
    if (record == null) {
      // SKIP IT
      OLogManager.instance().warn(this, "Record (%s) is null", rec);
      return null;
    }

    if (!(record instanceof ODocument)) {
      // SKIP IT
      OLogManager.instance().warn(this, "Found a record (%s) that is not an edge. Record: %s", rec, record);
      return null;
    }

    final ODocument value = rec.getRecord();

    if (value == null || value.getSchemaClass() == null)
      return null;

    final OrientEdge edge;
    if (value.getSchemaClass().isSubClassOf(OrientVertexType.CLASS_NAME)) {
      // DIRECT VERTEX, CREATE DUMMY EDGE
      if (connection.getKey() == Direction.OUT)
        edge = new OrientEdge(this.sourceVertex.graph, this.sourceVertex.getIdentity(), rec.getIdentity(), connection.getValue());
      else
        edge = new OrientEdge(this.sourceVertex.graph, rec.getIdentity(), this.sourceVertex.getIdentity(), connection.getValue());
    } else if (value.getSchemaClass().isSubClassOf(OrientEdgeType.CLASS_NAME)) {
      // EDGE
      edge = new OrientEdge(this.sourceVertex.graph, rec.getIdentity());
    } else
      throw new IllegalStateException("Invalid content found between connections:" + value);

    if (this.sourceVertex.settings.useVertexFieldsForEdgeLabels || edge.isLabeled(labels))
      return edge;

    return null;
  }

  public boolean filter(final OrientEdge iObject) {
    if (targetVertex != null && !targetVertex.equals(iObject.getVertex(connection.getKey().opposite())))
      return false;

    return this.sourceVertex.settings.useVertexFieldsForEdgeLabels || iObject.isLabeled(labels);
  }
}
