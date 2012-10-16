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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;

/**
 * Dijkstra's algorithms describes how to find the shortest path from one node to another node in a directed weighted graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionDijkstraCount extends OSQLFunctionMathAbstract {
  public static final String        NAME = "dijkstra";

  private OGraphDatabase            db;
  private Set<ODocument>            settledNodes;
  private Set<ODocument>            unSettledNodes;
  private Map<ODocument, ODocument> predecessors;
  private Map<ODocument, Float>     distance;
  private OIdentifiable             destinationVertex;
  private String                    weightFieldName;

  public OSQLFunctionDijkstraCount() {
    super(NAME, 2, 2);
  }

  public Object execute(OIdentifiable iCurrentRecord, final Object[] iParameters, OCommandExecutor iRequester) {
    final ODatabaseRecord currentDatabase = ODatabaseRecordThreadLocal.INSTANCE.get();
    db = (OGraphDatabase) (currentDatabase instanceof OGraphDatabase ? currentDatabase : new OGraphDatabase(
        (ODatabaseRecordTx) currentDatabase));

    destinationVertex = (OIdentifiable) iParameters[0];
    weightFieldName = (String) iParameters[1];

    settledNodes = new HashSet<ODocument>();
    unSettledNodes = new HashSet<ODocument>();
    distance = new HashMap<ODocument, Float>();
    predecessors = new HashMap<ODocument, ODocument>();
    distance.put((ODocument) iCurrentRecord.getRecord(), 0f);
    unSettledNodes.add((ODocument) iCurrentRecord.getRecord());

    while (unSettledNodes.size() > 0) {
      ODocument node = getMinimum(unSettledNodes);
      settledNodes.add(node);
      unSettledNodes.remove(node);
      findMinimalDistances(node);
    }

    return getPath(destinationVertex);
  }

  public boolean aggregateResults() {
    return false;
  }

  public String getSyntax() {
    return "Syntax error: dijkstra(destinationVertex, weightEdgeFieldName)";
  }

  @Override
  public Object getResult() {
    return settledNodes;
  }

  private void findMinimalDistances(final ODocument node) {
    final List<ODocument> adjacentNodes = getNeighbors(node);
    for (ODocument target : adjacentNodes) {
      if (getShortestDistance(target) > getShortestDistance(node) + getDistance(node, target)) {
        distance.put(target, getShortestDistance(node) + getDistance(node, target));
        predecessors.put(target, node);
        unSettledNodes.add(target);
      }
    }

  }

  private float getDistance(final ODocument node, final ODocument target) {
    Set<OIdentifiable> edges = db.getEdgesBetweenVertexes(node, target);
    final ODocument e = edges.iterator().next().getRecord();
    if (e != null) {
      final Object fieldValue = e.field(weightFieldName);
      if (fieldValue != null && fieldValue instanceof Number)
        return ((Number) fieldValue).floatValue();
    }
    return 0;
  }

  private List<ODocument> getNeighbors(final ODocument node) {
    final List<ODocument> neighbors = new ArrayList<ODocument>();
    final Set<OIdentifiable> outEdges = db.getOutEdges(node);
    for (OIdentifiable edge : outEdges) {
      final ODocument inVertex = db.getInVertex(edge);
      if (!isSettled(inVertex))
        neighbors.add(inVertex);

    }
    return neighbors;
  }

  private ODocument getMinimum(final Set<ODocument> vertexes) {
    ODocument minimum = null;
    for (ODocument vertex : vertexes) {
      if (minimum == null || getShortestDistance(vertex) < getShortestDistance(minimum))
        minimum = vertex;
    }
    return minimum;
  }

  private boolean isSettled(final ODocument vertex) {
    return settledNodes.contains(vertex);
  }

  private float getShortestDistance(final ODocument destination) {
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
