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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
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

  public OrientEdgeIterator(final OrientVertex iSourceVertex, final OrientVertex iTargetVertex, final Object iMultiValue, final Iterator<?> iterator,
      final OPair<Direction, String> connection, final String[] iLabels, final int iSize) {
    super(iterator, iSize, iMultiValue);
    this.sourceVertex = iSourceVertex;
    this.targetVertex = iTargetVertex;
    this.connection = connection;
    this.labels = iLabels;
  }

  @Override
  public OrientEdge createGraphElement(final Object iObject) {
    if (iObject instanceof OrientEdge)
      return (OrientEdge) iObject;

    final OIdentifiable rec = (OIdentifiable) iObject;

    if (rec == null) {
      // SKIP IT
      OLogManager.instance().warn(this, "Record (%s) is null", iObject);
      return null;
    }

    final ORecord record = rec.getRecord();
    if (record == null) {
      // SKIP IT
      OLogManager.instance().warn(this, "Record (%s) is null", rec);
      return null;
    }

    if (!(record instanceof ODocument)) {
      // SKIP IT
      OLogManager.instance().warn(this,
          "Found a record (%s) that is not an edge. Source vertex : %s, Target vertex : %s, Database : %s .", rec,
          sourceVertex != null ? sourceVertex.getIdentity() : null, targetVertex != null ? targetVertex.getIdentity() : null,
          record.getDatabase().getURL());
      return null;
    }

    final ODocument value = rec.getRecord();

    if (value == null) {
      return null;
    }

    OImmutableClass immutableSchema = ODocumentInternal.getImmutableSchemaClass(value);
    if (immutableSchema == null) {
      ODatabaseDocument db = value.getDatabaseIfDefined();
      if (db == null) {
        return null;
      }

      db.getMetadata().reload();
      immutableSchema = ODocumentInternal.getImmutableSchemaClass(value);
      if (immutableSchema == null) {
        return null;
      }
    }

    final OrientEdge edge;
    if (immutableSchema.isVertexType()) {
      // DIRECT VERTEX, CREATE DUMMY EDGE
      if (connection.getKey() == Direction.OUT)
        edge = new OrientEdge(this.sourceVertex.getGraph(), this.sourceVertex.getIdentity(), rec.getIdentity(),
            connection.getValue());
      else
        edge = new OrientEdge(this.sourceVertex.getGraph(), rec.getIdentity(), this.sourceVertex.getIdentity(),
            connection.getValue());
    } else if (immutableSchema.isEdgeType()) {
      // EDGE
      edge = new OrientEdge(this.sourceVertex.getGraph(), rec.getIdentity(), connection.getValue());
    } else
      throw new IllegalStateException("Invalid content found while iterating edges, value '" + value + "' is not an edge");

    if (this.sourceVertex.settings.isUseVertexFieldsForEdgeLabels() || edge.isLabeled(labels))
      return edge;

    return null;
  }

  public boolean filter(final OrientEdge iObject) {
    if (targetVertex != null && !targetVertex.equals(iObject.getVertex(connection.getKey().opposite())))
      return false;

    return this.sourceVertex.settings.isUseVertexFieldsForEdgeLabels() || iObject.isLabeled(labels);
  }

  @Override
  public boolean canUseMultiValueDirectly() {
    return true;
  }
}
