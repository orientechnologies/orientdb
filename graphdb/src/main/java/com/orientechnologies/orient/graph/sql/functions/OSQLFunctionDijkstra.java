/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.graph.sql.functions;

import java.util.Iterator;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDijkstra extends OSQLFunctionPathFinder<Float> {
  public static final String NAME = "dijkstra";
  private static final Float MIN  = new Float(0f);

  private String             paramWeightFieldName;

  public OSQLFunctionDijkstra() {
    super(NAME, 3, 4);
  }

  public Object execute(OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParameters, OCommandContext iContext) {
    final OrientBaseGraph graph = OGraphCommandExecutorSQLFactory.getGraph();
    try {

      final ORecordInternal<?> record = (ORecordInternal<?>) (iCurrentRecord != null ? iCurrentRecord.getRecord() : null);

      Object source = iParameters[0];
      if (OMultiValue.isMultiValue(source)) {
        if (OMultiValue.getSize(source) > 1)
          throw new IllegalArgumentException("Only one sourceVertex is allowed");
        source = OMultiValue.getFirstValue(source);
      }
      paramSourceVertex = graph.getVertex((OIdentifiable) OSQLHelper.getValue(source, record, iContext));

      Object dest = iParameters[1];
      if (OMultiValue.isMultiValue(dest)) {
        if (OMultiValue.getSize(dest) > 1)
          throw new IllegalArgumentException("Only one destinationVertex is allowed");
        dest = OMultiValue.getFirstValue(dest);
      }
      paramDestinationVertex = graph.getVertex((OIdentifiable) OSQLHelper.getValue(dest, record, iContext));

      paramWeightFieldName = (String) OSQLHelper.getValue(iParameters[2], record, iContext);
      if (iParameters.length > 3)
        paramDirection = Direction.valueOf(iParameters[3].toString().toUpperCase());

      return super.execute(iParameters, iContext);

    } finally {
      graph.shutdown();
    }
  }

  public String getSyntax() {
    return "Syntax error: dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<direction>])";
  }

  @Override
  protected Float getShortestDistance(final Vertex destination) {
    if (destination == null)
      return Float.MAX_VALUE;

    final Float d = distance.get(destination);
    return d == null ? Float.MAX_VALUE : d;
  }

  @Override
  protected Float getMinimumDistance() {
    return MIN;
  }

  protected Float getDistance(final Vertex node, final Vertex target) {
    final Iterator<Edge> edges = ((OrientVertex) node).getEdges((OrientVertex) target, paramDirection).iterator();
    if (edges.hasNext()) {
      final Edge e = edges.next();
      if (e != null) {
        final Object fieldValue = e.getProperty(paramWeightFieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Float)
            return (Float) fieldValue;
          else if (fieldValue instanceof Number)
            return ((Number) fieldValue).floatValue();
      }
    }
    return MIN;
  }

  @Override
  protected Float sumDistances(final Float iDistance1, final Float iDistance2) {
    return iDistance1.floatValue() + iDistance2.floatValue();
  }
}
