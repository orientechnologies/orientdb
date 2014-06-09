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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionShortestPath extends OSQLFunctionPathFinder {
  public static final String   NAME     = "shortestPath";

  protected static final float DISTANCE = 1f;

  public OSQLFunctionShortestPath() {
    super(NAME, 2, 3);
  }

  public Object execute(Object iThis, final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      final OCommandContext iContext) {
    final OrientBaseGraph graph = OGraphCommandExecutorSQLFactory.getGraph(false);

    final ORecordInternal<?> record = (ORecordInternal<?>) (iCurrentRecord != null ? iCurrentRecord.getRecord() : null);

    Object source = iParams[0];
    if (OMultiValue.isMultiValue(source)) {
      if (OMultiValue.getSize(source) > 1)
        throw new IllegalArgumentException("Only one sourceVertex is allowed");
      source = OMultiValue.getFirstValue(source);
    }
    paramSourceVertex = graph.getVertex(OSQLHelper.getValue(source, record, iContext));

    Object dest = iParams[1];
    if (OMultiValue.isMultiValue(dest)) {
      if (OMultiValue.getSize(dest) > 1)
        throw new IllegalArgumentException("Only one destinationVertex is allowed");
      dest = OMultiValue.getFirstValue(dest);
    }
    paramDestinationVertex = graph.getVertex(OSQLHelper.getValue(dest, record, iContext));

    if (iParams.length > 2)
      paramDirection = Direction.valueOf(iParams[2].toString().toUpperCase());

    return super.execute(iContext);
  }

  @Override
  protected float getDistance(final OrientVertex node, final OrientVertex target) {
    return DISTANCE;
  }

  public String getSyntax() {
    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>])";
  }
}
