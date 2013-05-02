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

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;

/**
 * Abstract class to find paths between nodes.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public abstract class OSQLFunctionPathFinder<T extends Comparable<T>> extends OSQLFunctionMathAbstract {
  protected OrientBaseGraph     db;
  protected Set<Vertex>         settledNodes;
  protected Set<Vertex>         unSettledNodes;
  protected Map<Vertex, Vertex> predecessors;
  protected Map<Vertex, T>      distance;

  protected Vertex              paramSourceVertex;
  protected Vertex              paramDestinationVertex;
  protected Direction           paramDirection = Direction.OUT;

  public OSQLFunctionPathFinder(final String iName, final int iMinParams, final int iMaxParams) {
    super(iName, iMinParams, iMaxParams);
  }

  protected abstract T getDistance(Vertex node, Vertex target);

  protected abstract T getShortestDistance(Vertex destination);

  protected abstract T getMinimumDistance();

  protected abstract T sumDistances(T iDistance1, T iDistance2);

  public Object execute(final Object[] iParameters, final OCommandContext iContext) {
    settledNodes = new HashSet<Vertex>();
    unSettledNodes = new HashSet<Vertex>();
    distance = new HashMap<Vertex, T>();
    predecessors = new HashMap<Vertex, Vertex>();
    distance.put(paramSourceVertex, getMinimumDistance());
    unSettledNodes.add(paramSourceVertex);

    while (continueTraversing()) {
      final Vertex node = getMinimum(unSettledNodes);
      settledNodes.add(node);
      unSettledNodes.remove(node);
      findMinimalDistances(node);
    }

    return getPath();
  }

  /*
   * This method returns the path from the source to the selected target and NULL if no path exists
   */
  public LinkedList<Vertex> getPath() {
    final LinkedList<Vertex> path = new LinkedList<Vertex>();
    Vertex step = paramDestinationVertex;
    // Check if a path exists
    if (predecessors.get(step) == null)
      return null;

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
    return getPath();
  }

  protected void findMinimalDistances(final Vertex node) {
    final List<Vertex> adjacentNodes = getNeighbors(node);
    for (Vertex target : adjacentNodes) {
      final T d = sumDistances(getShortestDistance(node), getDistance(node, target));

      if (getShortestDistance(target).compareTo(d) > 0) {
        distance.put(target, d);
        predecessors.put(target, node);
        unSettledNodes.add(target);
      }
    }

  }

  protected List<Vertex> getNeighbors(final Vertex node) {
    final List<Vertex> neighbors = new ArrayList<Vertex>();
    if (node != null) {
      for (Vertex v : node.getVertices(paramDirection))
        if (v != null && !isSettled(v))
          neighbors.add(v);
    }
    return neighbors;
  }

  protected Vertex getMinimum(final Set<Vertex> vertexes) {
    Vertex minimum = null;
    for (Vertex vertex : vertexes) {
      if (minimum == null || getShortestDistance(vertex).compareTo(getShortestDistance(minimum)) < 0)
        minimum = vertex;
    }
    return minimum;
  }

  protected boolean isSettled(final Vertex vertex) {
    return settledNodes.contains(vertex.getId());
  }

  protected boolean continueTraversing() {
    return unSettledNodes.size() > 0;
  }
}
