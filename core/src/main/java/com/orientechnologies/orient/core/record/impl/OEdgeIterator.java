/*
 *
 *  *  Copyright 2016 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.iterator.OLazyWrapperIterator;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.*;
import java.util.Iterator;

/** @author Luigi Dell'Aquila */
public class OEdgeIterator extends OLazyWrapperIterator<OEdge> {

  private final OVertex sourceVertex;
  private final OVertex targetVertex;
  private final OPair<ODirection, String> connection;
  private final String[] labels;

  public OEdgeIterator(
      final OVertex iSourceVertex,
      final Object iMultiValue,
      final Iterator<?> iterator,
      final OPair<ODirection, String> connection,
      final String[] iLabels,
      final int iSize) {
    this(iSourceVertex, null, iMultiValue, iterator, connection, iLabels, iSize);
  }

  public OEdgeIterator(
      final OVertex iSourceVertex,
      final OVertex iTargetVertex,
      final Object iMultiValue,
      final Iterator<?> iterator,
      final OPair<ODirection, String> connection,
      final String[] iLabels,
      final int iSize) {
    super(iterator, iSize, iMultiValue);
    this.sourceVertex = iSourceVertex;
    this.targetVertex = iTargetVertex;
    this.connection = connection;
    this.labels = iLabels;
  }

  public OEdge createGraphElement(final Object iObject) {
    if (iObject instanceof OElement && ((OElement) iObject).isEdge())
      return ((OElement) iObject).asEdge().get();

    final OIdentifiable rec = (OIdentifiable) iObject;

    if (rec == null) {
      // SKIP IT
      return null;
    }

    final ORecord record = rec.getRecord();
    if (record == null) {
      // SKIP IT
      OLogManager.instance().warn(this, "Record (%s) is null", rec);
      return null;
    }

    if (!(record instanceof OElement)) {
      // SKIP IT
      OLogManager.instance()
          .warn(
              this,
              "Found a record (%s) that is not an edge. Source vertex : %s, Target vertex : %s, Database : %s",
              rec,
              sourceVertex != null ? sourceVertex.getIdentity() : null,
              targetVertex != null ? targetVertex.getIdentity() : null,
              record.getDatabase().getURL());
      return null;
    }

    final OElement value = (OElement) record;

    final OEdge edge;
    if (value.isVertex()) {
      // DIRECT VERTEX, CREATE DUMMY EDGE
      ODatabaseDocumentInternal db = ODatabaseRecordThreadLocal.instance().getIfDefined();
      OClass clazz = null;
      if (db != null && connection.getValue() != null) {
        clazz = db.getMetadata().getImmutableSchemaSnapshot().getClass(connection.getValue());
      }
      if (connection.getKey() == ODirection.OUT) {
        edge =
            new OEdgeDelegate(
                this.sourceVertex, value.asVertex().get(), clazz, connection.getValue());
      } else {
        edge =
            new OEdgeDelegate(
                value.asVertex().get(), this.sourceVertex, clazz, connection.getValue());
      }
    } else if (value.isEdge()) {
      // EDGE
      edge = value.asEdge().get();
    } else
      throw new IllegalStateException(
          "Invalid content found while iterating edges, value '" + value + "' is not an edge");

    return edge;
  }

  public boolean filter(final OEdge iObject) {
    if (targetVertex != null
        && !targetVertex.equals(iObject.getVertex(connection.getKey().opposite()))) return false;

    return iObject.isLabeled(labels);
  }

  @Override
  public boolean canUseMultiValueDirectly() {
    return true;
  }
}
