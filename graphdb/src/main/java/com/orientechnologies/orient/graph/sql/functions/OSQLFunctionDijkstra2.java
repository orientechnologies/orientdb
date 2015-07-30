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

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
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
 * The first parameter is the source record. 
 * The second parameter is the destination record. 
 * The third parameter is the edge type i. e. the subclass of E considered for routing.
 * The fourth parameter is the name of the property that represents the 'weight' or 'cost'.
 *  
 * If an edge has no weight field or the weight field is null this edge is not considered for routing.
 * 
 * @author Martin Hulin
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDijkstra2 extends OSQLFunctionPathFinder {
  public static final String NAME = "dijkstra2";

  private String             		paramEdgeType; //+
  private String             paramWeightFieldName;

  public OSQLFunctionDijkstra2() {
    super(NAME, 4, 5);
  }

  public LinkedList<OrientVertex> execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, OCommandContext iContext) {
    final OModifiableBoolean shutdownFlag = new OModifiableBoolean();
    ODatabaseDocumentInternal curDb = ODatabaseRecordThreadLocal.INSTANCE.get();
    final OrientBaseGraph graph = OGraphCommandExecutorSQLFactory.getGraph(false, shutdownFlag);
    try {

      final ORecord record = (ORecord) (iCurrentRecord != null ? iCurrentRecord.getRecord() : null);

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

      paramEdgeType = OStringSerializerHelper.getStringContent(iParams[2]); // +
      
      paramWeightFieldName = OStringSerializerHelper.getStringContent(iParams[3]);
      if (iParams.length > 4)
        paramDirection = Direction.valueOf(iParams[4].toString().toUpperCase());

      return super.execute(iContext);
    } finally {
      if (shutdownFlag.getValue())
        graph.shutdown(false);
      ODatabaseRecordThreadLocal.INSTANCE.set(curDb);
    }
  }

  public String getSyntax() {
    return "dijkstra2(<sourceVertex>, <destinationVertex>, <edgeType>, <weightEdgeFieldName>, [<direction>])";
  }

  protected float getDistance(final OrientVertex node, final OrientVertex target) {
    final Iterator<Edge> edges = node.getEdges(target, paramDirection, paramEdgeType).iterator();
    float bestWeight = Float.MAX_VALUE; // if no edge of paramEdgeType and with field paramWeightFieldName exist set distance to maximal value. 
    while (edges.hasNext()) { // changed from 'if' to 'while': if more than one edge exist take that one with the least weight
      final Edge e = edges.next();
      float edgeWeight = Float.MAX_VALUE;
      if (e != null) {
        final Object fieldValue = e.getProperty(paramWeightFieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Float)
            edgeWeight = (Float) fieldValue;
          else if (fieldValue instanceof Number)
            edgeWeight = ((Number) fieldValue).floatValue();
      }
      if (edgeWeight < bestWeight) bestWeight = edgeWeight;
    }
    return bestWeight;
  }
	
@Override
protected Set<OrientVertex> getNeighbors(final Vertex node) {
    context.incrementVariable("getNeighbors");    
  	Direction opDirection;
    final Set<OrientVertex> neighbors = new HashSet<OrientVertex>();
    if (node != null) {
    	for (Edge e: node.getEdges(paramDirection, paramEdgeType)) { //changed: regard only edges of edgeType
    		if (e.getProperty(paramWeightFieldName) != null) {
    			if (e.getVertex(Direction.IN).equals(node)) opDirection = Direction.OUT;
    			else opDirection = Direction.IN;

    			final OrientVertex ov = (OrientVertex) e.getVertex(opDirection);
    			if (ov != null && isNotSettled(ov))
    				neighbors.add(ov);
    		}
    	}
    }
    return neighbors;
}

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }
}
