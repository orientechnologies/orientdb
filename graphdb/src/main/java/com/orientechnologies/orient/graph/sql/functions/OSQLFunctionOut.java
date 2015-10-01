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
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.OSizeable;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

/**
 * Gets the outgoing Vertices of current Vertex.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionOut extends OSQLFunctionMoveFiltered {
  public static final String NAME               = "out";
  int                        supernodeThreshold = 1000;

  public OSQLFunctionOut() {
    super(NAME, 0, -1);
  }

  @Override
  protected Object move(final OrientBaseGraph graph, final OIdentifiable iRecord, final String[] iLabels) {
    return v2v(graph, iRecord, Direction.OUT, iLabels);
  }

  protected Object move(final OrientBaseGraph graph, final OIdentifiable iRecord, final String[] iLabels,
      Iterable<OIdentifiable> iPossibleResults) {
    if (iPossibleResults == null) {
      return v2v(graph, iRecord, Direction.OUT, iLabels);
    }

    if (!iPossibleResults.iterator().hasNext()) {
      return Collections.emptyList();
    }

    Object edges = v2e(graph, iRecord, Direction.OUT, iLabels);
    if (edges instanceof OSizeable) {
      int size = ((OSizeable) edges).size();
      if (size > supernodeThreshold) {
        Object result = fetchFromIndex(graph, iRecord, iPossibleResults, iLabels);
        if (result != null) {
          return result;
        }
      }

    }

    return v2v(graph, iRecord, Direction.OUT, iLabels);
  }

  private Object fetchFromIndex(OrientBaseGraph graph, OIdentifiable iFrom, Iterable<OIdentifiable> iTo, String[] iEdgeTypes) {
    String edgeClassName = null;
    if (iEdgeTypes == null) {
      edgeClassName = "E";
    } else if (iEdgeTypes.length == 1) {
      edgeClassName = iEdgeTypes[0];
    } else {
      return null;
    }
    OClass edgeClass = graph.getRawGraph().getMetadata().getSchema().getClass(edgeClassName);
    if (edgeClass == null) {
      return null;
    }
    Set<OIndex<?>> indexes = edgeClass.getInvolvedIndexes("out", "in");
    if (indexes == null || indexes.size() == 0) {
      return null;
    }
    OIndex index = indexes.iterator().next();

    OMultiCollectionIterator<OrientVertex> result = new OMultiCollectionIterator<OrientVertex>();
    for (OIdentifiable to : iTo) {
      OCompositeKey key = new OCompositeKey(iFrom, to);
      Object indexResult = index.get(key);
      if (indexResult instanceof OIdentifiable) {
        indexResult = Collections.singleton(indexResult);
      }
      Set<OIdentifiable> identities = new HashSet<OIdentifiable>();
      for (OIdentifiable edge : ((Iterable<OrientEdge>) indexResult)) {
        identities.add((OIdentifiable) ((ODocument) edge.getRecord()).rawField("in"));
      }
      result.add(identities);
    }

    return result;
  }

}
