package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiCollectionIterator;
import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.OEdgeToVertexIterable;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed
 * graph.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionShortestPath extends OSQLFunctionMathAbstract {
  public static final String NAME = "shortestPath";
  public static final String PARAM_MAX_DEPTH = "maxDepth";

  protected static final float DISTANCE = 1f;

  public OSQLFunctionShortestPath() {
    super(NAME, 2, 5);
  }

  private class OShortestPathContext {
    private OVertex sourceVertex;
    private OVertex destinationVertex;
    private ODirection directionLeft = ODirection.BOTH;
    private ODirection directionRight = ODirection.BOTH;

    private String edgeType;
    private String[] edgeTypeParam;

    private ArrayDeque<OVertex> queueLeft = new ArrayDeque<>();
    private ArrayDeque<OVertex> queueRight = new ArrayDeque<>();

    private final Set<ORID> leftVisited = new HashSet<ORID>();
    private final Set<ORID> rightVisited = new HashSet<ORID>();

    private final Map<ORID, ORID> previouses = new HashMap<ORID, ORID>();
    private final Map<ORID, ORID> nexts = new HashMap<ORID, ORID>();

    private OVertex current;
    private OVertex currentRight;
    public Integer maxDepth;
    /** option that decides whether or not to return the edge information */
    public Boolean edge;
  }

  public List<ORID> execute(
      Object iThis,
      final OIdentifiable iCurrentRecord,
      final Object iCurrentResult,
      final Object[] iParams,
      final OCommandContext iContext) {

    final ORecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;

    final OShortestPathContext ctx = new OShortestPathContext();

    Object source = iParams[0];
    source = getSingleItem(source);
    if (source == null) {
      throw new IllegalArgumentException("Only one sourceVertex is allowed");
    }
    source = OSQLHelper.getValue(source, record, iContext);
    if (source instanceof OIdentifiable) {
      OElement elem = ((OIdentifiable) source).getRecord();
      if (elem == null || !elem.isVertex()) {
        throw new IllegalArgumentException("The sourceVertex must be a vertex record");
      }
      ctx.sourceVertex = elem.asVertex().get();
    } else {
      throw new IllegalArgumentException("The sourceVertex must be a vertex record");
    }

    Object dest = iParams[1];
    dest = getSingleItem(dest);
    if (dest == null) {
      throw new IllegalArgumentException("Only one destinationVertex is allowed");
    }
    dest = OSQLHelper.getValue(dest, record, iContext);
    if (dest instanceof OIdentifiable) {
      OElement elem = ((OIdentifiable) dest).getRecord();
      if (elem == null || !elem.isVertex()) {
        throw new IllegalArgumentException("The destinationVertex must be a vertex record");
      }
      ctx.destinationVertex = elem.asVertex().get();
    } else {
      throw new IllegalArgumentException("The destinationVertex must be a vertex record");
    }

    if (ctx.sourceVertex.equals(ctx.destinationVertex)) {
      final List<ORID> result = new ArrayList<ORID>(1);
      result.add(ctx.destinationVertex.getIdentity());
      return result;
    }

    if (iParams.length > 2 && iParams[2] != null) {
      ctx.directionLeft = ODirection.valueOf(iParams[2].toString().toUpperCase(Locale.ENGLISH));
    }
    if (ctx.directionLeft == ODirection.OUT) {
      ctx.directionRight = ODirection.IN;
    } else if (ctx.directionLeft == ODirection.IN) {
      ctx.directionRight = ODirection.OUT;
    }

    ctx.edgeType = null;
    if (iParams.length > 3) {

      Object param = iParams[3];
      if (param instanceof Collection
          && ((Collection) param).stream().allMatch(x -> x instanceof String)) {
        ctx.edgeType = ((Collection<String>) param).stream().collect(Collectors.joining(","));
        ctx.edgeTypeParam = (String[]) ((Collection) param).toArray(new String[0]);
      } else {
        ctx.edgeType = param == null ? null : "" + param;
        ctx.edgeTypeParam = new String[] {ctx.edgeType};
      }
    } else {
      ctx.edgeTypeParam = new String[] {null};
    }

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
      if (ctx.queueLeft.isEmpty() || ctx.queueRight.isEmpty()) break;

      if (Thread.interrupted())
        throw new OCommandExecutionException("The shortestPath() function has been interrupted");

      if (!OCommandExecutorAbstract.checkInterruption(iContext)) break;

      List<ORID> neighborIdentity;

      if (ctx.queueLeft.size() <= ctx.queueRight.size()) {
        // START EVALUATING FROM LEFT
        neighborIdentity = walkLeft(ctx);
        if (neighborIdentity != null) return neighborIdentity;
        depth++;
        if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
          break;
        }

        if (ctx.queueLeft.isEmpty()) break;

        neighborIdentity = walkRight(ctx);
        if (neighborIdentity != null) return neighborIdentity;

      } else {

        // START EVALUATING FROM RIGHT
        neighborIdentity = walkRight(ctx);
        if (neighborIdentity != null) return neighborIdentity;

        depth++;
        if (ctx.maxDepth != null && ctx.maxDepth <= depth) {
          break;
        }

        if (ctx.queueRight.isEmpty()) break;

        neighborIdentity = walkLeft(ctx);
        if (neighborIdentity != null) return neighborIdentity;
      }

      depth++;
    }
    return new ArrayList<ORID>();
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
      Boolean withEdge = toBoolean(mapParams.get("edge"));
      ctx.edge = Boolean.TRUE.equals(withEdge) ? Boolean.TRUE : Boolean.FALSE;
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
        return Integer.parseInt(fromObject.toString());
      } catch (NumberFormatException ignore) {
      }
    }
    return null;
  }

  /**
   * @return
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private Boolean toBoolean(Object fromObject) {
    if (fromObject == null) {
      return null;
    }
    if (fromObject instanceof Boolean) {
      return (Boolean) fromObject;
    }
    if (fromObject instanceof String) {
      try {
        return Boolean.parseBoolean(fromObject.toString());
      } catch (NumberFormatException ignore) {
      }
    }
    return null;
  }

  /**
   * get adjacent vertices and edges
   *
   * @param srcVertex
   * @param direction
   * @param types
   * @return
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private ORawPair<Iterable<OVertex>, Iterable<OEdge>> getVerticesAndEdges(
      OVertex srcVertex, ODirection direction, String... types) {
    if (direction == ODirection.BOTH) {
      OMultiCollectionIterator<OVertex> vertexIterator = new OMultiCollectionIterator<>();
      OMultiCollectionIterator<OEdge> edgeIterator = new OMultiCollectionIterator<>();
      ORawPair<Iterable<OVertex>, Iterable<OEdge>> pair1 =
          getVerticesAndEdges(srcVertex, ODirection.OUT, types);
      ORawPair<Iterable<OVertex>, Iterable<OEdge>> pair2 =
          getVerticesAndEdges(srcVertex, ODirection.IN, types);
      vertexIterator.add(pair1.first);
      vertexIterator.add(pair2.first);
      edgeIterator.add(pair1.second);
      edgeIterator.add(pair2.second);
      return new ORawPair<>(vertexIterator, edgeIterator);
    } else {
      Iterable<OEdge> edges1 = srcVertex.getEdges(direction, types);
      Iterable<OEdge> edges2 = srcVertex.getEdges(direction, types);
      return new ORawPair<>(new OEdgeToVertexIterable(edges1, direction), edges2);
    }
  }

  /**
   * get adjacent vertices and edges
   *
   * @param srcVertex
   * @param direction
   * @return
   * @author Thomas Young (YJJThomasYoung@hotmail.com)
   */
  private ORawPair<Iterable<OVertex>, Iterable<OEdge>> getVerticesAndEdges(
      OVertex srcVertex, ODirection direction) {
    return getVerticesAndEdges(srcVertex, direction, (String[]) null);
  }

  public String getSyntax() {
    return "shortestPath(<sourceVertex>, <destinationVertex>, [<direction>, [ <edgeTypeAsString> ]])";
  }

  protected List<ORID> walkLeft(final OSQLFunctionShortestPath.OShortestPathContext ctx) {
    ArrayDeque<OVertex> nextLevelQueue = new ArrayDeque<>();
    if (!Boolean.TRUE.equals(ctx.edge)) {
      while (!ctx.queueLeft.isEmpty()) {
        ctx.current = ctx.queueLeft.poll();

        Iterable<OVertex> neighbors;
        if (ctx.edgeType == null) {
          neighbors = ctx.current.getVertices(ctx.directionLeft);
        } else {
          neighbors = ctx.current.getVertices(ctx.directionLeft, ctx.edgeTypeParam);
        }
        for (OVertex neighbor : neighbors) {
          final OVertex v = (OVertex) neighbor;
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
    } else {
      while (!ctx.queueLeft.isEmpty()) {
        ctx.current = ctx.queueLeft.poll();

        ORawPair<Iterable<OVertex>, Iterable<OEdge>> neighbors;
        if (ctx.edgeType == null) {
          neighbors = getVerticesAndEdges(ctx.current, ctx.directionLeft);
        } else {
          neighbors = getVerticesAndEdges(ctx.current, ctx.directionLeft, ctx.edgeTypeParam);
        }
        Iterator<OVertex> vertexIterator = neighbors.first.iterator();
        Iterator<OEdge> edgeIterator = neighbors.second.iterator();
        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
          OVertex v = vertexIterator.next();
          final ORID neighborVertexIdentity = v.getIdentity();
          final ORID neighborEdgeIdentity = edgeIterator.next().getIdentity();

          if (ctx.rightVisited.contains(neighborVertexIdentity)) {
            ctx.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.previouses.put(neighborEdgeIdentity, ctx.current.getIdentity());
            return computePath(ctx.previouses, ctx.nexts, neighborVertexIdentity);
          }
          if (!ctx.leftVisited.contains(neighborVertexIdentity)) {
            ctx.previouses.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.previouses.put(neighborEdgeIdentity, ctx.current.getIdentity());

            nextLevelQueue.offer(v);
            ctx.leftVisited.add(neighborVertexIdentity);
          }
        }
      }
    }
    ctx.queueLeft = nextLevelQueue;
    return null;
  }

  protected List<ORID> walkRight(final OSQLFunctionShortestPath.OShortestPathContext ctx) {
    final ArrayDeque<OVertex> nextLevelQueue = new ArrayDeque<>();
    if (!Boolean.TRUE.equals(ctx.edge)) {
      while (!ctx.queueRight.isEmpty()) {
        ctx.currentRight = ctx.queueRight.poll();

        Iterable<OVertex> neighbors;
        if (ctx.edgeType == null) {
          neighbors = ctx.currentRight.getVertices(ctx.directionRight);
        } else {
          neighbors = ctx.currentRight.getVertices(ctx.directionRight, ctx.edgeTypeParam);
        }
        for (OVertex neighbor : neighbors) {
          final OVertex v = (OVertex) neighbor;
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
    } else {
      while (!ctx.queueRight.isEmpty()) {
        ctx.currentRight = ctx.queueRight.poll();

        ORawPair<Iterable<OVertex>, Iterable<OEdge>> neighbors;
        if (ctx.edgeType == null) {
          neighbors = getVerticesAndEdges(ctx.currentRight, ctx.directionRight);
        } else {
          neighbors = getVerticesAndEdges(ctx.currentRight, ctx.directionRight, ctx.edgeTypeParam);
        }

        Iterator<OVertex> vertexIterator = neighbors.first.iterator();
        Iterator<OEdge> edgeIterator = neighbors.second.iterator();
        while (vertexIterator.hasNext() && edgeIterator.hasNext()) {
          final OVertex v = vertexIterator.next();
          final ORID neighborVertexIdentity = v.getIdentity();
          final ORID neighborEdgeIdentity = edgeIterator.next().getIdentity();

          if (ctx.leftVisited.contains(neighborVertexIdentity)) {
            ctx.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.nexts.put(neighborEdgeIdentity, ctx.currentRight.getIdentity());
            return computePath(ctx.previouses, ctx.nexts, neighborVertexIdentity);
          }
          if (!ctx.rightVisited.contains(neighborVertexIdentity)) {
            ctx.nexts.put(neighborVertexIdentity, neighborEdgeIdentity);
            ctx.nexts.put(neighborEdgeIdentity, ctx.currentRight.getIdentity());

            nextLevelQueue.offer(v);
            ctx.rightVisited.add(neighborVertexIdentity);
          }
        }
      }
    }
    ctx.queueRight = nextLevelQueue;
    return null;
  }

  private List<ORID> computePath(
      final Map<ORID, ORID> leftDistances,
      final Map<ORID, ORID> rightDistances,
      final ORID neighbor) {
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
