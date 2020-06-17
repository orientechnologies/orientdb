/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.graph.sql.functions;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Locale;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * A*'s algorithm describes how to find the cheapest path from one node to another node in a
 * directed weighted graph with husrestic function.
 *
 * <p>The first parameter is source record. The second parameter is destination record. The third
 * parameter is a name of property that represents 'weight' and fourth represnts the map of options.
 *
 * <p>If property is not defined in edge or is null, distance between vertexes are 0 .
 *
 * @author Saeed Tabrizi (saeed a_t nowcando.com)
 * @deprecated see {@link com.orientechnologies.orient.core.sql.functions.graph.OSQLFunctionAstar}
 *     instead
 */
@Deprecated
public class OSQLFunctionAstar extends OSQLFunctionHeuristicPathFinderAbstract {
  public static final String NAME = "astar";

  private String paramWeightFieldName = "weight";
  private long currentDepth = 0;
  protected Set<OrientVertex> closedSet = new HashSet<OrientVertex>();
  protected Map<OrientVertex, OrientVertex> cameFrom = new HashMap<OrientVertex, OrientVertex>();

  protected Map<OrientVertex, Double> gScore = new HashMap<OrientVertex, Double>();
  protected Map<OrientVertex, Double> fScore = new HashMap<OrientVertex, Double>();
  protected PriorityQueue<OrientVertex> open =
      new PriorityQueue<OrientVertex>(
          1,
          new Comparator<OrientVertex>() {

            public int compare(OrientVertex nodeA, OrientVertex nodeB) {
              return Double.compare(fScore.get(nodeA), fScore.get(nodeB));
            }
          });

  public OSQLFunctionAstar() {
    super(NAME, 3, 4);
  }

  public LinkedList<OrientVertex> execute(
      final Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      final OCommandContext iContext) {
    context = iContext;
    final OSQLFunctionAstar context = this;
    return OGraphCommandExecutorSQLFactory.runWithAnyGraph(
        new OGraphCommandExecutorSQLFactory.GraphCallBack<LinkedList<OrientVertex>>() {
          @Override
          public LinkedList<OrientVertex> call(final OrientBaseGraph graph) {

            final ORecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;

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

            paramWeightFieldName = OIOUtils.getStringContent(iParams[2]);

            if (iParams.length > 3) {
              bindAdditionalParams(iParams[3], context);
            }
            iContext.setVariable("getNeighbors", 0);
            if (paramSourceVertex == null || paramDestinationVertex == null) {
              return new LinkedList<>();
            }
            return internalExecute(iContext, graph);
          }
        });
  }

  private LinkedList<OrientVertex> internalExecute(
      final OCommandContext iContext, OrientBaseGraph graph) {

    OrientVertex start = paramSourceVertex;
    OrientVertex goal = paramDestinationVertex;

    open.add(start);

    // The cost of going from start to start is zero.
    gScore.put(start, 0.0);
    // For the first node, that value is completely heuristic.
    fScore.put(start, getHeuristicCost(start, null, goal));

    while (!open.isEmpty()) {
      OrientVertex current = open.poll();

      // we discussed about this feature in
      // https://github.com/orientechnologies/orientdb/pull/6002#issuecomment-212492687
      if (paramEmptyIfMaxDepth == true && currentDepth >= paramMaxDepth) {
        route.clear(); // to ensure our result is empty
        return getPath();
      }
      // if start and goal vertex is equal so return current path from  cameFrom hash map
      if (current.getIdentity().equals(goal.getIdentity()) || currentDepth >= paramMaxDepth) {

        while (current != null) {
          route.add(0, current);
          current = cameFrom.get(current);
        }
        return getPath();
      }

      closedSet.add(current);
      for (OrientEdge neighborEdge : getNeighborEdges(current)) {

        OrientVertex neighbor = getNeighbor(current, neighborEdge, graph);
        if (neighbor == null) {
          continue;
        }
        // Ignore the neighbor which is already evaluated.
        if (closedSet.contains(neighbor)) {
          continue;
        }
        // The distance from start to a neighbor
        double tentativeGScore = gScore.get(current) + getDistance(neighborEdge);
        boolean contains = open.contains(neighbor);

        if (!contains || tentativeGScore < gScore.get(neighbor)) {
          gScore.put(neighbor, tentativeGScore);
          fScore.put(neighbor, tentativeGScore + getHeuristicCost(neighbor, current, goal));

          if (contains) {
            open.remove(neighbor);
          }
          open.offer(neighbor);
          cameFrom.put(neighbor, current);
        }
      }

      // Increment Depth Level
      currentDepth++;
    }

    return getPath();
  }

  private OrientVertex getNeighbor(
      OrientVertex current, OrientEdge neighborEdge, OrientBaseGraph graph) {
    if (neighborEdge.getOutVertex().equals(current)) {
      return toVertex(neighborEdge.getInVertex(), graph);
    }
    return toVertex(neighborEdge.getOutVertex(), graph);
  }

  private OrientVertex toVertex(OIdentifiable outVertex, OrientBaseGraph graph) {
    if (outVertex == null) {
      return null;
    }
    if (outVertex instanceof OrientVertex) {
      return (OrientVertex) outVertex;
    }
    return graph.getVertex(outVertex);
  }

  protected Set<OrientEdge> getNeighborEdges(final OrientVertex node) {
    context.incrementVariable("getNeighbors");

    final Set<OrientEdge> neighbors = new HashSet<OrientEdge>();
    if (node != null) {
      for (Edge v : node.getEdges(paramDirection, paramEdgeTypeNames)) {
        final OrientEdge ov = (OrientEdge) v;
        if (ov != null) neighbors.add(ov);
      }
    }
    return neighbors;
  }

  private void bindAdditionalParams(Object additionalParams, OSQLFunctionAstar ctx) {
    if (additionalParams == null) {
      return;
    }
    Map<String, Object> mapParams = null;
    if (additionalParams instanceof Map) {
      mapParams = (Map) additionalParams;
    } else if (additionalParams instanceof OIdentifiable) {
      mapParams = ((ODocument) ((OIdentifiable) additionalParams).getRecord()).toMap();
    }
    if (mapParams != null) {
      ctx.paramEdgeTypeNames = stringArray(mapParams.get(OSQLFunctionAstar.PARAM_EDGE_TYPE_NAMES));
      ctx.paramVertexAxisNames =
          stringArray(mapParams.get(OSQLFunctionAstar.PARAM_VERTEX_AXIS_NAMES));
      if (mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION) != null) {
        if (mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION) instanceof String) {
          ctx.paramDirection =
              Direction.valueOf(
                  stringOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION), "OUT")
                      .toUpperCase(Locale.ENGLISH));
        } else {
          ctx.paramDirection = (Direction) mapParams.get(OSQLFunctionAstar.PARAM_DIRECTION);
        }
      }

      ctx.paramParallel = booleanOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_PARALLEL), false);
      ctx.paramMaxDepth =
          longOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_MAX_DEPTH), ctx.paramMaxDepth);
      ctx.paramEmptyIfMaxDepth =
          booleanOrDefault(
              mapParams.get(OSQLFunctionAstar.PARAM_EMPTY_IF_MAX_DEPTH), ctx.paramEmptyIfMaxDepth);
      ctx.paramTieBreaker =
          booleanOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_TIE_BREAKER), ctx.paramTieBreaker);
      ctx.paramDFactor =
          doubleOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_D_FACTOR), ctx.paramDFactor);
      if (mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA) != null) {
        if (mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA) instanceof String) {
          ctx.paramHeuristicFormula =
              HeuristicFormula.valueOf(
                  stringOrDefault(
                          mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA), "MANHATAN")
                      .toUpperCase(Locale.ENGLISH));
        } else {
          ctx.paramHeuristicFormula =
              (HeuristicFormula) mapParams.get(OSQLFunctionAstar.PARAM_HEURISTIC_FORMULA);
        }
      }

      ctx.paramCustomHeuristicFormula =
          stringOrDefault(mapParams.get(OSQLFunctionAstar.PARAM_CUSTOM_HEURISTIC_FORMULA), "");
    }
  }

  public String getSyntax() {
    return "astar(<sourceVertex>, <destinationVertex>, <weightEdgeFieldName>, [<options>]) \n"
        + " // options  : {direction:\"OUT\",edgeTypeNames:[] , vertexAxisNames:[] , parallel : false , "
        + "tieBreaker:true,maxDepth:99999,dFactor:1.0,customHeuristicFormula:'custom_Function_Name_here'  }";
  }

  @Override
  public Object getResult() {
    return getPath();
  }

  @Override
  protected double getDistance(
      final OrientVertex node, final OrientVertex parent, final OrientVertex target) {
    final Iterator<Edge> edges = node.getEdges(target, paramDirection).iterator();
    if (edges.hasNext()) {
      final Edge e = edges.next();
      if (e != null) {
        final Object fieldValue = e.getProperty(paramWeightFieldName);
        if (fieldValue != null)
          if (fieldValue instanceof Float) return (Float) fieldValue;
          else if (fieldValue instanceof Number) return ((Number) fieldValue).doubleValue();
      }
    }
    return MIN;
  }

  protected double getDistance(final OrientEdge edge) {
    if (edge != null) {
      final Object fieldValue = edge.getProperty(paramWeightFieldName);
      if (fieldValue != null)
        if (fieldValue instanceof Float) return (Float) fieldValue;
        else if (fieldValue instanceof Number) return ((Number) fieldValue).doubleValue();
    }

    return MIN;
  }

  @Override
  public boolean aggregateResults() {
    return false;
  }

  @Override
  protected double getHeuristicCost(
      final OrientVertex node, OrientVertex parent, final OrientVertex target) {
    double hresult = 0.0;

    if (paramVertexAxisNames.length == 0) {
      return hresult;
    } else if (paramVertexAxisNames.length == 1) {
      double n = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]), 0.0);
      double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]), 0.0);
      hresult = getSimpleHeuristicCost(n, g, paramDFactor);
    } else if (paramVertexAxisNames.length == 2) {
      if (parent == null) parent = node;
      double sx = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[0]), 0);
      double sy = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[1]), 0);
      double nx = doubleOrDefault(node.getProperty(paramVertexAxisNames[0]), 0);
      double ny = doubleOrDefault(node.getProperty(paramVertexAxisNames[1]), 0);
      double px = doubleOrDefault(parent.getProperty(paramVertexAxisNames[0]), 0);
      double py = doubleOrDefault(parent.getProperty(paramVertexAxisNames[1]), 0);
      double gx = doubleOrDefault(target.getProperty(paramVertexAxisNames[0]), 0);
      double gy = doubleOrDefault(target.getProperty(paramVertexAxisNames[1]), 0);

      switch (paramHeuristicFormula) {
        case MANHATAN:
          hresult = getManhatanHeuristicCost(nx, ny, gx, gy, paramDFactor);
          break;
        case MAXAXIS:
          hresult = getMaxAxisHeuristicCost(nx, ny, gx, gy, paramDFactor);
          break;
        case DIAGONAL:
          hresult = getDiagonalHeuristicCost(nx, ny, gx, gy, paramDFactor);
          break;
        case EUCLIDEAN:
          hresult = getEuclideanHeuristicCost(nx, ny, gx, gy, paramDFactor);
          break;
        case EUCLIDEANNOSQR:
          hresult = getEuclideanNoSQRHeuristicCost(nx, ny, gx, gy, paramDFactor);
          break;
        case CUSTOM:
          hresult =
              getCustomHeuristicCost(
                  paramCustomHeuristicFormula,
                  paramVertexAxisNames,
                  paramSourceVertex,
                  paramDestinationVertex,
                  node,
                  parent,
                  currentDepth,
                  paramDFactor);
          break;
      }
      if (paramTieBreaker) {
        hresult = getTieBreakingHeuristicCost(px, py, sx, sy, gx, gy, hresult);
      }

    } else {
      Map<String, Double> sList = new HashMap<String, Double>();
      Map<String, Double> cList = new HashMap<String, Double>();
      Map<String, Double> pList = new HashMap<String, Double>();
      Map<String, Double> gList = new HashMap<String, Double>();
      parent = parent == null ? node : parent;
      for (int i = 0; i < paramVertexAxisNames.length; i++) {
        Double s = doubleOrDefault(paramSourceVertex.getProperty(paramVertexAxisNames[i]), 0);
        Double c = doubleOrDefault(node.getProperty(paramVertexAxisNames[i]), 0);
        Double g = doubleOrDefault(target.getProperty(paramVertexAxisNames[i]), 0);
        Double p = doubleOrDefault(parent.getProperty(paramVertexAxisNames[i]), 0);
        if (s != null) sList.put(paramVertexAxisNames[i], s);
        if (c != null) cList.put(paramVertexAxisNames[i], s);
        if (g != null) gList.put(paramVertexAxisNames[i], g);
        if (p != null) pList.put(paramVertexAxisNames[i], p);
      }
      switch (paramHeuristicFormula) {
        case MANHATAN:
          hresult =
              getManhatanHeuristicCost(
                  paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
          break;
        case MAXAXIS:
          hresult =
              getMaxAxisHeuristicCost(
                  paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
          break;
        case DIAGONAL:
          hresult =
              getDiagonalHeuristicCost(
                  paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
          break;
        case EUCLIDEAN:
          hresult =
              getEuclideanHeuristicCost(
                  paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
          break;
        case EUCLIDEANNOSQR:
          hresult =
              getEuclideanNoSQRHeuristicCost(
                  paramVertexAxisNames, sList, cList, pList, gList, currentDepth, paramDFactor);
          break;
        case CUSTOM:
          hresult =
              getCustomHeuristicCost(
                  paramCustomHeuristicFormula,
                  paramVertexAxisNames,
                  paramSourceVertex,
                  paramDestinationVertex,
                  node,
                  parent,
                  currentDepth,
                  paramDFactor);
          break;
      }
      if (paramTieBreaker) {
        hresult =
            getTieBreakingHeuristicCost(
                paramVertexAxisNames, sList, cList, pList, gList, currentDepth, hresult);
      }
    }

    return hresult;
  }

  @Override
  protected boolean isVariableEdgeWeight() {
    return true;
  }
}
