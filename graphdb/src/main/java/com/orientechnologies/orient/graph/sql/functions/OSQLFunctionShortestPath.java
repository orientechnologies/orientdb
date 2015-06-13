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
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import com.orientechnologies.orient.graph.sql.OGraphCommandExecutorSQLFactory;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.*;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 * 
 * @author Luca Garulli (l.garulli--at--orientechnologies.com)
 * 
 */
public class OSQLFunctionShortestPath extends OSQLFunctionMathAbstract {
  public static final String   NAME     = "shortestPath";

  protected static final float DISTANCE = 1f;

  public OSQLFunctionShortestPath() {
    super(NAME, 2, 4);
  }

  public List<ORID> execute(Object iThis, final OIdentifiable iCurrentRecord, Object iCurrentResult, final Object[] iParams,
      final OCommandContext iContext) {
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
      OrientVertex sourceVertex = graph.getVertex(OSQLHelper.getValue(source, record, iContext));

      Object dest = iParams[1];
      if (OMultiValue.isMultiValue(dest)) {
        if (OMultiValue.getSize(dest) > 1)
          throw new IllegalArgumentException("Only one destinationVertex is allowed");
        dest = OMultiValue.getFirstValue(dest);
      }
      OrientVertex destinationVertex = graph.getVertex(OSQLHelper.getValue(dest, record, iContext));

      if (sourceVertex.equals(destinationVertex)) {
        final List<ORID> result = new ArrayList<ORID>(1);
        result.add(destinationVertex.getIdentity());
        return result;
      }

      Direction direction = Direction.BOTH;
      Direction reverseDirection = Direction.BOTH;
      if (iParams.length > 2 && iParams[2] != null) {
        direction = Direction.valueOf(iParams[2].toString().toUpperCase());
      }
      if (direction == Direction.OUT) {
        reverseDirection = Direction.IN;
      } else if (direction == Direction.IN) {
        reverseDirection = Direction.OUT;
      }

      Object edgeType = null;
      if (iParams.length > 3) {
        edgeType = iParams[3];
      }

      ArrayDeque<OrientVertex> queue1 = new ArrayDeque<OrientVertex>();
      ArrayDeque<OrientVertex> queue2 = new ArrayDeque<OrientVertex>();

      final Set<ORID> visited = new HashSet<ORID>();

      final Set<ORID> leftVisited = new HashSet<ORID>();
      final Set<ORID> rightVisited = new HashSet<ORID>();

      final Map<ORID, ORID> previouses = new HashMap<ORID, ORID>();
      final Map<ORID, ORID> nexts = new HashMap<ORID, ORID>();

      queue1.add(sourceVertex);
      visited.add(sourceVertex.getIdentity());

      queue2.add(destinationVertex);
      visited.add(destinationVertex.getIdentity());

      OrientVertex current;
      OrientVertex reverseCurrent;

      while (true) {
        if (queue1.isEmpty() && queue2.isEmpty()) {
          break;
        }

        ArrayDeque<OrientVertex> nextLevelQueue = new ArrayDeque<OrientVertex>();
        while (!queue1.isEmpty()) {
          current = queue1.poll();

          Iterable<Vertex> neighbors;
          if (edgeType == null) {
            neighbors = current.getVertices(direction);
          } else {
            neighbors = current.getVertices(direction, new String[] { "" + edgeType });
          }
          for (Vertex neighbor : neighbors) {
            final OrientVertex v = (OrientVertex) neighbor;
            final ORID neighborIdentity = v.getIdentity();

            if (rightVisited.contains(neighborIdentity)) {
              previouses.put(neighborIdentity, current.getIdentity());
              return computePath(previouses, nexts, neighborIdentity);
            }
            if (!visited.contains(neighborIdentity)) {
              previouses.put(neighborIdentity, current.getIdentity());

              nextLevelQueue.offer(v);
              visited.add(neighborIdentity);
              leftVisited.add(neighborIdentity);
            }

          }
        }
        queue1 = nextLevelQueue;
        nextLevelQueue = new ArrayDeque<OrientVertex>();

        while (!queue2.isEmpty()) {
          reverseCurrent = queue2.poll();

          Iterable<Vertex> neighbors;
          if (edgeType == null) {
            neighbors = reverseCurrent.getVertices(reverseDirection);
          } else {
            neighbors = reverseCurrent.getVertices(reverseDirection, new String[] { "" + edgeType });
          }
          for (Vertex neighbor : neighbors) {
            final OrientVertex v = (OrientVertex) neighbor;
            final ORID neighborIdentity = v.getIdentity();

            if (leftVisited.contains(neighborIdentity)) {
              nexts.put(neighborIdentity, reverseCurrent.getIdentity());
              return computePath(previouses, nexts, neighborIdentity);
            }
            if (!visited.contains(neighborIdentity)) {

              nexts.put(neighborIdentity, reverseCurrent.getIdentity());

              nextLevelQueue.offer(v);
              visited.add(neighborIdentity);
              rightVisited.add(neighborIdentity);
            }

          }
        }
        queue2 = nextLevelQueue;
      }
      return new ArrayList<ORID>();
    } finally {
      if (shutdownFlag.getValue())
        graph.shutdown(false);
      ODatabaseRecordThreadLocal.INSTANCE.set(curDb);
    }
  }

  public String getSyntax() {
    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>, [ <edgeTypeAsString> ]])";
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
