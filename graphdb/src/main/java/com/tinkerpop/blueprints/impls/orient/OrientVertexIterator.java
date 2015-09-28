/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;

import java.util.Collection;
import java.util.Iterator;

public class OrientVertexIterator extends OLazyWrapperIterator<Vertex> {
  private final OrientVertex             vertex;
  private final String[]                 iLabels;
  private final OPair<Direction, String> connection;

  public OrientVertexIterator(final OrientVertex orientVertex, final Object iMultiValue, final Iterator<?> iterator,
      final OPair<Direction, String> connection, final String[] iLabels, final int iSize) {
    super(iterator, iSize, iMultiValue);
    this.vertex = orientVertex;
    this.connection = connection;
    this.iLabels = iLabels;
  }

  @Override
  public Vertex createGraphElement(final Object iObject) {
    if (iObject instanceof OrientVertex)
      return (OrientVertex) iObject;

    if (iObject == null) {
      return null;
    }

    final ORecord rec = ((OIdentifiable) iObject).getRecord();

    if (rec == null || !(rec instanceof ODocument))
      return null;

    final ODocument value = (ODocument) rec;

    final OrientVertex v;
    OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(value);
    if (immutableClass.isVertexType()) {
      // DIRECT VERTEX
      v = new OrientVertex(vertex.getGraph(), value);
    } else if (immutableClass.isEdgeType()) {
      // EDGE
      if (vertex.settings.isUseVertexFieldsForEdgeLabels() || OrientEdge.isLabeled(OrientEdge.getRecordLabel(value), iLabels))
        v = new OrientVertex(vertex.getGraph(), OrientEdge.getConnection(value, connection.getKey().opposite()));
      else
        v = null;
    } else
      throw new IllegalStateException("Invalid content found between connections: " + value);

    return v;
  }

  @Override
  public OIdentifiable getGraphElementRecord(final Object iObject) {
    final ORecord rec = ((OIdentifiable) iObject).getRecord();

    if (rec == null || !(rec instanceof ODocument))
      return null;

    final ODocument value = (ODocument) rec;

    final OIdentifiable v;
    OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(value);
    if (immutableClass.isVertexType()) {
      // DIRECT VERTEX
      v = value;
    } else if (immutableClass.isEdgeType()) {
      // EDGE
      if (vertex.settings.isUseVertexFieldsForEdgeLabels() || OrientEdge.isLabeled(OrientEdge.getRecordLabel(value), iLabels))
        v = OrientEdge.getConnection(value, connection.getKey().opposite());
      else
        v = null;
    } else
      throw new IllegalStateException("Invalid content found between connections: " + value);

    return v;
  }

  @Override
  public boolean canUseMultiValueDirectly() {
    if (multiValue instanceof Collection) {
      if ((((Collection) multiValue).isEmpty()) || isVertex((OIdentifiable) ((Collection) multiValue).iterator().next()))
        return true;
    } else if (multiValue instanceof ORidBag) {
      if ((((ORidBag) multiValue).isEmpty()) || isVertex(((ORidBag) multiValue).iterator().next()))
        return true;
    }

    return false;
  }

  public boolean filter(final Vertex iObject) {
    return !(iObject instanceof OrientVertex && ((OrientVertex) iObject).getRecord() == null);
  }

  private boolean isVertex(final OIdentifiable iObject) {
    final ORecord rec = iObject.getRecord();

    if (rec == null || !(rec instanceof ODocument))
      return false;

    final ODocument value = (ODocument) rec;

    final OIdentifiable v;
    OImmutableClass immutableClass = ODocumentInternal.getImmutableSchemaClass(value);
    if (immutableClass.isVertexType()) {
      // DIRECT VERTEX
      return true;
    } else if (immutableClass.isEdgeType()) {
      return false;
    }

    throw new IllegalStateException("Invalid content found between connections: " + value);
  }
}
