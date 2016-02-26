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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 *
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 */
public class OSQLFunctionShortestPath extends OSQLFunctionMathAbstract {
  public static final String NAME = "shortestPath";
  public static final String PARAM_MAX_DEPTH = "maxDepth";

  protected static final float DISTANCE = 1f;

  public OSQLFunctionShortestPath() {
    super(NAME, 2, 5);
  }

  private class OShortestPathContext {
    OrientVertex sourceVertex;
    OrientVertex destinationVertex;
    Direction directionLeft  = Direction.BOTH;
    Direction directionRight = Direction.BOTH;

    String   edgeType;
    String[] edgeTypeParam;

    ArrayDeque<OrientVertex> queueLeft  = new ArrayDeque<OrientVertex>();
    ArrayDeque<OrientVertex> queueRight = new ArrayDeque<OrientVertex>();

    final Set<ORID> leftVisited  = new HashSet<ORID>();
    final Set<ORID> rightVisited = new HashSet<ORID>();

    final Map<ORID, ORID> previouses = new HashMap<ORID, ORID>();
    final Map<ORID, ORID> nexts      = new HashMap<ORID, ORID>();

    OrientVertex current;
    OrientVertex currentRight;
    public Integer maxDepth;
  }

  public List<ORID> execute(Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final OCommandContext iContext) {

    return OGraphCommandExecutorSQLFactory.runWithAnyGraph(new OGraphCommandExecutorSQLFactory.GraphCallBack<List<ORID>>() {
      @Override
      public List<ORID> call(final OrientBaseGraph graph) {
        final ORecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;

        final OShortestPathContext ctx = new OShortestPathContext();

        Object source = iParams[0];
        if (OMultiValue.isMultiValue(source)) {
          if (OMultiValue.getSize(source) > 1)
            throw new IllegalArgumentException("Only one sourceVertex is allowed");
          source = OMultiValue.getFirstValue(source);
        }
        ctx.sourceVertex = graph.getVertex(OSQLHelper.getValue(source, record, iContext));

        Object dest = iParams[1];
        if (OMultiValue.isMultiValue(dest)) {
          if (OMultiValue.getSize(dest) > 1)
            throw new IllegalArgumentException("Only one destinationVertex is allowed");
          dest = OMultiValue.getFirstValue(dest);
        }
        ctx.destinationVertex = graph.getVertex(OSQLHelper.getValue(dest, record, iContext));

        if (ctx.sourceVertex.equals(ctx.destinationVertex)) {
          final List<ORID> result = new ArrayList<ORID>(1);
          result.add(ctx.destinationVertex.getIdentity());
          return result;
        }

        if (iParams.length > 2 && iParams[2] != null) {
          ctx.directionLeft = Direction.valueOf(iParams[2].toString().toUpperCase());
        }
        if (ctx.directionLeft == Direction.OUT) {
          ctx.directionRight = Direction.IN;
        } else if (ctx.directionLeft == Direction.IN) {
          ctx.directionRight = Direction.OUT;
        }

        ctx.edgeType = null;
        if (iParams.length > 3) {
          ctx.edgeType = iParams[3] == null ? null : "" + iParams[3];
        }
        ctx.edgeTypeParam = new String[] { ctx.edgeType };

        if (iParams.length > 4) {
          bindAdditionalParams(iParams[4], ctx);
        }

        ctx.queueLeft.add(ctx.sourceVertex);
        ctx.leftVisited.add(ctx.sourceVertex.getIdentity());

        ctx.queueRight.add(ctx.destinationVertex);
        ctx.rightVisited.add(ctx.destinationVertex.getIdentity());

        int depth = 1;
        while (true) {
          if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
            break;
          }
          if (ctx.queueLeft.isEmpty() || ctx.queueRight.isEmpty())
            break;

          if (Thread.interrupted())
            throw new OCommandExecutionException("The shortestPath() function has been interrupted");

          if (!OCommandExecutorAbstract.checkInterruption(iContext))
            break;

          List<ORID> neighborIdentity;

          if (ctx.queueLeft.size() <= ctx.queueRight.size()) {
            // START EVALUATING FROM LEFT
            neighborIdentity = walkLeft(ctx);
            if (neighborIdentity != null)
              return neighborIdentity;
            depth++;
            if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
              break;
            }

            if (ctx.queueLeft.isEmpty())
              break;

            neighborIdentity = walkRight(ctx);
            if (neighborIdentity != null)
              return neighborIdentity;

          } else {

            // START EVALUATING FROM RIGHT
            neighborIdentity = walkRight(ctx);
            if (neighborIdentity != null)
              return neighborIdentity;

            depth++;
            if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
              break;
            }

            if (ctx.queueRight.isEmpty())
              break;

            neighborIdentity = walkLeft(ctx);
            if (neighborIdentity != null)
              return neighborIdentity;
          }

          depth++;
        }
        return new ArrayList<ORID>();
      }
    });
  }

  private void bindAdditionalParams(Object additionalParams, OShortestPathContext ctx) {
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
      ctx.maxDepth = integer(mapParams.get("maxDepth"));
    }
  }

  private Integer integer(Object fromObject) {
    if (fromObject == null) {
      return null;
    }
    if (fromObject instanceof Number) {
      return ((Number) fromObject).intValue();
    }
    if (fromObject instanceof String) {
      try {
        return Integer.parseInt((String) fromObject);
      } catch (Exception e) {
      }
    }
    return null;
  }

  public String getSyntax() {
    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>, [ <edgeTypeAsString> ]])";
  }

  protected List<ORID> walkLeft(final OSQLFunctionShortestPath.OShortestPathContext ctx) {
    ArrayDeque<OrientVertex> nextLevelQueue = new ArrayDeque<OrientVertex>();
    while (!ctx.queueLeft.isEmpty()) {
      ctx.current = ctx.queueLeft.poll();

      Iterable<Vertex> neighbors;
      if (ctx.edgeType == null) {
        neighbors = ctx.current.getVertices(ctx.directionLeft);
      } else {
        neighbors = ctx.current.getVertices(ctx.directionLeft, ctx.edgeTypeParam);
      }
      for (Vertex neighbor : neighbors) {
        final OrientVertex v = (OrientVertex) neighbor;
        final ORID neighborIdentity = v.getIdentity();

        if (ctx.rightVisited.contains(neighborIdentity)) {
          ctx.previouses.put(neighborIdentity, ctx.current.getIdentity());
          return computePath(ctx.previouses, ctx.nexts, neighborIdentity);
        }
        if (!ctx.leftVisited.contains(neighborIdentity)) {
          ctx.previouses.put(neighborIdentity, ctx.current.getIdentity());

          nextLevelQueue.offer(v);
          ctx.leftVisited.add(neighborIdentity);
        }

      }
    }
    ctx.queueLeft = nextLevelQueue;
    return null;
  }

  protected List<ORID> walkRight(final OSQLFunctionShortestPath.OShortestPathContext ctx) {
    final ArrayDeque<OrientVertex> nextLevelQueue = new ArrayDeque<OrientVertex>();

    while (!ctx.queueRight.isEmpty()) {
      ctx.currentRight = ctx.queueRight.poll();

      Iterable<Vertex> neighbors;
      if (ctx.edgeType == null) {
        neighbors = ctx.currentRight.getVertices(ctx.directionRight);
      } else {
        neighbors = ctx.currentRight.getVertices(ctx.directionRight, ctx.edgeTypeParam);
      }
      for (Vertex neighbor : neighbors) {
        final OrientVertex v = (OrientVertex) neighbor;
        final ORID neighborIdentity = v.getIdentity();

        if (ctx.leftVisited.contains(neighborIdentity)) {
          ctx.nexts.put(neighborIdentity, ctx.currentRight.getIdentity());
          return computePath(ctx.previouses, ctx.nexts, neighborIdentity);
        }
        if (!ctx.rightVisited.contains(neighborIdentity)) {

          ctx.nexts.put(neighborIdentity, ctx.currentRight.getIdentity());

          nextLevelQueue.offer(v);
          ctx.rightVisited.add(neighborIdentity);
        }

      }
    }
    ctx.queueRight = nextLevelQueue;
    return null;
  }

  private List<ORID> computePath(final Map<ORID, ORID> leftDistances, final Map<ORID, ORID> rightDistances, final ORID neighbor) {
    final List<ORID> result = new ArrayList<ORID>();

    ORID current = neighbor;
    while (current != null) {
      result.add(0, current);
      current = leftDistances.get(current);
    }

    current = neighbor;
    while (current != null) {
      current = rightDistances.get(current);
      if (current != null) {
        result.add(current);
      }
    }

    return result;
  }
}
