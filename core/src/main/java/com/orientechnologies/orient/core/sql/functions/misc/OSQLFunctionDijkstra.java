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
package com.orientechnologies.orient.core.sql.functions.misc;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;

/**
 * Dijkstra's algorithms describes how to find the shortest path from one node to another node in a directed weighted graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDijkstra extends OSQLFunctionMathAbstract {
  public static final String                NAME = "dijkstra";

  private OGraphDatabase                    db;
  private Set<OIdentifiable>                settledNodes;
  private Set<OIdentifiable>                unSettledNodes;
  private Map<OIdentifiable, OIdentifiable> predecessors;
  private Map<OIdentifiable, Float>         distance;

  private OIdentifiable                     paramSourceVertex;
  private OIdentifiable                     paramDestinationVertex;
  private String                            paramWeightFieldName;

  public OSQLFunctionDijkstra() {
    super(NAME, 2, 3);
  }

  public Object execute(OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
    final ODatabaseRecord currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();
    db = (OGraphDatabase) (currentDatabase instanceof OGraphDatabase ? currentDatabase : new OGraphDatabase(
        (ODatabaseRecordTx) currentDatabase));

    paramSourceVertex = (OIdentifiable) OSQLHelper.getValue(iParameters[0], (ORecordInternal<?>) iCurrentRecord.getRecord(),
        iRequester.getContext());
    paramDestinationVertex = (OIdentifiable) OSQLHelper.getValue(iParameters[1], (ORecordInternal<?>) iCurrentRecord.getRecord(),
        iRequester.getContext());
    paramWeightFieldName = (String) OSQLHelper.getValue(iParameters[2], (ORecordInternal<?>) iCurrentRecord.getRecord(),
        iRequester.getContext());

    settledNodes = new HashSet<OIdentifiable>();
    unSettledNodes = new HashSet<OIdentifiable>();
    distance = new HashMap<OIdentifiable, Float>();
    predecessors = new HashMap<OIdentifiable, OIdentifiable>();
    distance.put(paramSourceVertex, 0f);
    unSettledNodes.add(paramSourceVertex);

    iCurrentRecord.getRecord();

    while (unSettledNodes.size() > 0) {
      OIdentifiable node = getMinimum(unSettledNodes);
      settledNodes.add(node);
      unSettledNodes.remove(node);
      findMinimalDistances(node);
    }

    return getPath(paramDestinationVertex);
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "Syntax error: dijkstra(sourceVertex, destinationVertex, weightEdgeFieldName)";
  }

  @Override
  public Object getResult() {
    return settledNodes;
  }

  private void findMinimalDistances(final OIdentifiable node) {
    final List<OIdentifiable> adjacentNodes = getNeighbors(node);
    for (OIdentifiable target : adjacentNodes) {
      if (getShortestDistance(target) > getShortestDistance(node) + getDistance(node, target)) {
        distance.put(target, getShortestDistance(node) + getDistance(node, target));
        predecessors.put(target, node);
        unSettledNodes.add(target);
      }
    }

  }

  private float getDistance(final OIdentifiable node, final OIdentifiable target) {
    Set<OIdentifiable> edges = db.getEdgesBetweenVertexes(node, target);
    final ODocument e = edges.iterator().next().getRecord();
    if (e != null) {
      final Object fieldValue = e.field(paramWeightFieldName);
      if (fieldValue != null && fieldValue instanceof Number)
        return ((Number) fieldValue).floatValue();
    }
    return 0;
  }

  private List<OIdentifiable> getNeighbors(final OIdentifiable node) {
    final List<OIdentifiable> neighbors = new ArrayList<OIdentifiable>();
    final Set<OIdentifiable> outEdges = db.getOutEdges(node);
    for (OIdentifiable edge : outEdges) {
      final ODocument inVertex = db.getInVertex(edge);
      if (!isSettled(inVertex))
        neighbors.add(inVertex);

    }
    return neighbors;
  }

  private OIdentifiable getMinimum(final Set<OIdentifiable> vertexes) {
    OIdentifiable minimum = null;
    for (OIdentifiable vertex : vertexes) {
      if (minimum == null || getShortestDistance(vertex) < getShortestDistance(minimum))
        minimum = vertex;
    }
    return minimum;
  }

  private boolean isSettled(final OIdentifiable vertex) {
    return settledNodes.contains(vertex);
  }

  private float getShortestDistance(final OIdentifiable destination) {
    final Float d = distance.get(destination);
    return d == null ? Float.MAX_VALUE : d;
  }

  /*
   * This method returns the path from the source to the selected target and NULL if no path exists
   */
  public LinkedList<OIdentifiable> getPath(final OIdentifiable target) {
    final LinkedList<OIdentifiable> path = new LinkedList<OIdentifiable>();
    OIdentifiable step = target;
    // Check if a path exists
    if (predecessors.get(step) == null) {
      return null;
    }
    path.add(step);
    while (predecessors.get(step) != null) {
      step = predecessors.get(step);
      path.add(step);
    }
    // Put it into the correct order
    Collections.reverse(path);
    return path;
  }

}
