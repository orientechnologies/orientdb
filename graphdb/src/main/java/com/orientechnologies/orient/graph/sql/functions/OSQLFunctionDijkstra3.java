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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

/**
 * Dijkstra's algorithm describes how to find the cheapest path from one node to another node in a directed weighted graph.
 * 
 * This implementation should  replace the current implementation "dijkstra" in OSQLFunctionDijkstra.
 * The names dijkstra3 and OSQLFunctionDijkstra3 are only used in order not to overwrite the original version accidentally
 * because this version has a different behavior in some cases:
 * - If source and destination coincide return the trivial path with only one vertex instead of an empty path
 * - If an edge has no 'weight' field or the 'weight' field is null this edge is not considered for routing (gets the maximal
 *   possible weight)
 * - The result is List<ORID> instead of LinkedList<OrientVertex>
 * - If no path exists the result is an empty list instead of null
 *  
 * The first parameter is the source record. The second parameter is the destination record.
 * The third parameter is a name of property that represents 'weight'.
 * The forth parameter is optional: the direction of edges considered for the search; default is OUT
 * The fifth parameter is optional: the edge type i. e. the subclass of E considered for routing; default is E (all edges)
 * 
 * If an edge has no weight field or the weight field is null this edge is not considered for routing.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * @author Martin Hulin
 * 
 */
public class OSQLFunctionDijkstra3 extends OSQLFunctionMathAbstract {
  /* The names dijkstra3 and OSQLFunctionDijkstra3 are only used in order not to overwrite the original version accidentally
  * because this version has a different behavior in some cases. At last it could replace the curren implementation.
  */
  public static final String NAME = "dijkstra3";

  private static class VertexWithDistance implements Comparable<Object>{
	private OrientVertex vertex;
	private float distance;
	public VertexWithDistance(OrientVertex vertex, float distance) {
	  this.vertex = vertex;
	  this.distance = distance;
	}
	@Override
	public int compareTo(Object vwd) {
	  float diff = this.distance - ((VertexWithDistance) vwd).distance;
	  if (diff < 0) return -1;
	  else if (diff > 0) return 1;
	  else return 0;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		VertexWithDistance other = (VertexWithDistance) obj;
		if (distance != other.distance)
			return false;
		if (vertex == null) {
			if (other.vertex != null)
				return false;
		} else if (!vertex.equals(other.vertex))
			return false;
		return true;
	}
	@Override
	public String toString() {
		return "[" + vertex + ", " + distance + "]";
	}
  }

  private String           					edgeType = "E"; //default value is "all edges" if parameter edgeType is omitted
  private String           					paramWeightFieldName;
  private OrientVertex     					sourceVertex;
  private OrientVertex             	destinationVertex;
  private Direction                	direction  = Direction.OUT;
  private PriorityQueue<VertexWithDistance> queue; // unsettled nodes in a priority queue
  private HashSet<ORID> 					  unSettledNodes; // the same as queue but without distances; purpose: fast response whether a vertex is in queue
  private HashMap<ORID, Float> 			distances;
  private HashMap<ORID, ORID> 			predecessors;
  protected OCommandContext         context;

  public OSQLFunctionDijkstra3() {
    super(NAME, 3, 5);
  }

  public List<ORID> execute(Object iThis, OIdentifiable iCurrentRecord, Object iCurrentResult,
      final Object[] iParams, OCommandContext iContext) {
	context = iContext;
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
	  sourceVertex = graph.getVertex(OSQLHelper.getValue(source, record, iContext));
	  Object dest = iParams[1];
	  if (OMultiValue.isMultiValue(dest)) {
	    if (OMultiValue.getSize(dest) > 1)
	      throw new IllegalArgumentException("Only one destinationVertex is allowed");
	    dest = OMultiValue.getFirstValue(dest);
	  }
	  destinationVertex = graph.getVertex(OSQLHelper.getValue(dest, record, iContext));
	
	  paramWeightFieldName = OStringSerializerHelper.getStringContent(iParams[2]);
	  if (iParams.length > 3)
	    direction = Direction.valueOf(iParams[3].toString().toUpperCase());
	
	  if (iParams.length > 4)
		  edgeType = OStringSerializerHelper.getStringContent(iParams[4]); // +
	 
	  //the algorithm
	  if (sourceVertex.equals(destinationVertex)) {
	    final List<ORID> result = new ArrayList<ORID>(1);
	    result.add(destinationVertex.getIdentity());
	    return result; // if source and destination coincide return the trivial path with only one vertex
	  }
	  queue = new PriorityQueue<VertexWithDistance>();
	  unSettledNodes = new HashSet<ORID> ();
	  distances = new HashMap <ORID, Float>();
	  predecessors = new HashMap<ORID, ORID>();
	  distances.put(sourceVertex.getIdentity(), 0f);
	  queue.add(new VertexWithDistance(sourceVertex, 0f));
	  unSettledNodes.add(sourceVertex.getIdentity());
	
	  while (!queue.isEmpty()) {
	    final OrientVertex node = pollVertexFromQueue ();

	    if (node.getIdentity().equals(destinationVertex.getIdentity()))
	      // terminate if destination is found
	      break;
	      
	    for (VertexWithDistance neighbor : getNeighbors(node)) {
	      final float alternativeDistance = getActualDistance(node) + neighbor.distance;
	      if (getActualDistance(neighbor.vertex) > alternativeDistance) {
	        distances.put(neighbor.vertex.getIdentity(), alternativeDistance);
	        predecessors.put(neighbor.vertex.getIdentity(), node.getIdentity());
	        neighbor.distance = alternativeDistance; // the distance in queue is the total distance from sourceVertex to the neighbor of node
	        addVertexToQueue(neighbor);
	      }
	    }
	    
	    if (!context.checkTimeout())
	      break;
	  }
	  
	  // return the path
	  final LinkedList<ORID> path = new LinkedList<ORID>();
	  ORID step = destinationVertex.getIdentity();
	  // Check if a path exists
	  if (predecessors.get(step) == null)
	      return path; // return empty path

	  path.addFirst(step);
	  while (predecessors.get(step) != null) {
	    step = predecessors.get(step);
	    path.addFirst(step);
	  }
	  return path;

    } finally {
      if (shutdownFlag.getValue())
        graph.shutdown(false);
      ODatabaseRecordThreadLocal.INSTANCE.set(curDb);
    }
  }

  private OrientVertex pollVertexFromQueue() {
	 OrientVertex node = queue.poll().vertex;
	 unSettledNodes.remove(node.getIdentity());
	 return node;
  }

  private void addVertexToQueue(VertexWithDistance neighbor) {
      queue.add(neighbor);
      unSettledNodes.add(neighbor.vertex.getIdentity());
  }

  public String getSyntax() {
    return "dijkstra(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<direction>, [<edgeType>]])";
  }

  private float getActualDistance(final OrientVertex v) {
	if (v == null) return Float.MAX_VALUE;

	final Float d = distances.get(v.getIdentity());
	return d == null ? Float.MAX_VALUE : d;
  }
  
  /*
   * This function retrieves all unsettled neighbors of "node" considering only edges of type "paramEdgeType"
   * getNeibors in OSQLPathFinder.java considers all edges
   */
  private Set<VertexWithDistance> getNeighbors(final Vertex node) {
	Direction opDirection;
    final Set<VertexWithDistance> neighbors = new HashSet<VertexWithDistance>();
    if (node != null) {
  	  for (Edge e: node.getEdges(direction, edgeType)) { // regard only edges of edgeType
  	    float edgeWeight = Float.MAX_VALUE;
  	    if (e != null) {
  	      final Object fieldValue = e.getProperty(paramWeightFieldName);
  	      if (fieldValue != null)
  	        if (fieldValue instanceof Float)
  	          edgeWeight = (Float) fieldValue;
  	        else if (fieldValue instanceof Number)
  	          edgeWeight = ((Number) fieldValue).floatValue();
  	    }
 		
  		if (e.getVertex(Direction.IN).equals(node))
  		  opDirection = Direction.OUT;
  		else opDirection = Direction.IN;

  		final OrientVertex ov = (OrientVertex) e.getVertex(opDirection); // get the vertex on the other end of the edge e
  		if (ov != null && isNotSettled(ov.getIdentity())) 
  		  neighbors.add(new VertexWithDistance (ov, edgeWeight)); // the distance in neighbors is the direct distance from node to a neighbor
  	  }
    }
    return neighbors;
  }
  private boolean isNotSettled(final ORID v) {
	return unSettledNodes.contains(v) || !distances.containsKey(v);
  }    
}
