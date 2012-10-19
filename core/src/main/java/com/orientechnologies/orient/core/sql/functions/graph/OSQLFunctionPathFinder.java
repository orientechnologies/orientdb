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
package com.orientechnologies.orient.core.sql.functions.graph;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandExecutor;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase.DIRECTION;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;

/**
 * Abstract class to find paths between nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunctionPathFinder<T extends Comparable<T>> extends OSQLFunctionMathAbstract {
  protected OGraphDatabase                    db;
  protected Set<OIdentifiable>                settledNodes;
  protected Set<OIdentifiable>                unSettledNodes;
  protected Map<OIdentifiable, OIdentifiable> predecessors;
  protected Map<OIdentifiable, T>             distance;

  protected OIdentifiable                     paramSourceVertex;
  protected OIdentifiable                     paramDestinationVertex;
  protected OGraphDatabase.DIRECTION          paramDirection = DIRECTION.OUT;

  public OSQLFunctionPathFinder(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected abstract T getDistance(OIdentifiable node, OIdentifiable target);

  protected abstract T getShortestDistance(OIdentifiable destination);

  protected abstract T getMinimumDistance();

  protected abstract T sumDistances(T iDistance1, T iDistance2);

  public Object execute(final Object[] iParameters, final OCommandExecutor iRequester) {
    settledNodes = new HashSet<OIdentifiable>();
    unSettledNodes = new HashSet<OIdentifiable>();
    distance = new HashMap<OIdentifiable, T>();
    predecessors = new HashMap<OIdentifiable, OIdentifiable>();
    distance.put(paramSourceVertex, getMinimumDistance());
    unSettledNodes.add(paramSourceVertex);

    while (continueTraversing()) {
      final OIdentifiable node = getMinimum(unSettledNodes);
      settledNodes.add(node);
      unSettledNodes.remove(node);
      findMinimalDistances(node);
    }

    return getPath(paramDestinationVertex);
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

  public boolean aggregateResults() {
    return false;
  }

  @Override
  public Object getResult() {
    return settledNodes;
  }

  protected void findMinimalDistances(final OIdentifiable node) {
    final List<OIdentifiable> adjacentNodes = getNeighbors(node);
    for (OIdentifiable target : adjacentNodes) {
      final T d = sumDistances(getShortestDistance(node), getDistance(node, target));

      if (getShortestDistance(target).compareTo(d) > 0) {
        distance.put(target, d);
        predecessors.put(target, node);
        unSettledNodes.add(target);
      }
    }

  }

  protected List<OIdentifiable> getNeighbors(final OIdentifiable node) {
    final List<OIdentifiable> neighbors = new ArrayList<OIdentifiable>();

    if (paramDirection == DIRECTION.BOTH || paramDirection == DIRECTION.OUT)
      for (OIdentifiable edge : db.getOutEdges(node)) {
        final ODocument inVertex = db.getInVertex(edge);
        if (inVertex != null && !isSettled(inVertex))
          neighbors.add(inVertex);
      }

    if (paramDirection == DIRECTION.BOTH || paramDirection == DIRECTION.IN)
      for (OIdentifiable edge : db.getInEdges(node)) {
        final ODocument outVertex = db.getOutVertex(edge);
        if (outVertex != null && !isSettled(outVertex))
          neighbors.add(outVertex);
      }

    return neighbors;
  }

  protected OIdentifiable getMinimum(final Set<OIdentifiable> vertexes) {
    OIdentifiable minimum = null;
    for (OIdentifiable vertex : vertexes) {
      if (minimum == null || getShortestDistance(vertex).compareTo(getShortestDistance(minimum)) < 0)
        minimum = vertex;
    }
    return minimum;
  }

  protected boolean isSettled(final OIdentifiable vertex) {
    return settledNodes.contains(vertex);
  }

  protected boolean continueTraversing() {
    return unSettledNodes.size() > 0;
  }
}
