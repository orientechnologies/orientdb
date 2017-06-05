package com.orientechnologies.orient.core.sql.functions.graph;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OSQLHelper;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.functions.math.OSQLFunctionMathAbstract;

import java.util.*;

/**
 * Shortest path algorithm to find the shortest path from one node to another node in a directed graph.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OSQLFunctionShortestPath extends OSQLFunctionMathAbstract {
  public static final String NAME            = "shortestPath";
  public static final String PARAM_MAX_DEPTH = "maxDepth";

  protected static final float DISTANCE = 1f;

  public OSQLFunctionShortestPath() {
    super(NAME, 2, 5);
  }

  private class OShortestPathContext {
    OVertex sourceVertex;
    OVertex destinationVertex;
    ODirection directionLeft  = ODirection.BOTH;
    ODirection directionRight = ODirection.BOTH;

    String   edgeType;
    String[] edgeTypeParam;

    ArrayDeque<OVertex> queueLeft  = new ArrayDeque<>();
    ArrayDeque<OVertex> queueRight = new ArrayDeque<>();

    final Set<ORID> leftVisited  = new HashSet<ORID>();
    final Set<ORID> rightVisited = new HashSet<ORID>();

    final Map<ORID, ORID> previouses = new HashMap<ORID, ORID>();
    final Map<ORID, ORID> nexts      = new HashMap<ORID, ORID>();

    OVertex current;
    OVertex currentRight;
    public Integer maxDepth;
  }

  public List<ORID> execute(Object iThis, final OIdentifiable iCurrentRecord, final Object iCurrentResult, final Object[] iParams,
      final OCommandContext iContext) {

    final ORecord record = iCurrentRecord != null ? iCurrentRecord.getRecord() : null;

    final OShortestPathContext ctx = new OShortestPathContext();

    Object source = iParams[0];
    if (OMultiValue.isMultiValue(source)) {
      if (OMultiValue.getSize(source) > 1)
        throw new IllegalArgumentException("Only one sourceVertex is allowed");
      source = OMultiValue.getFirstValue(source);
      if (source instanceof OResult && ((OResult) source).isElement()) {
        source = ((OResult) source).getElement().get();
      }
    }
    source = OSQLHelper.getValue(source, record, iContext);
    if (source instanceof OIdentifiable) {
      OElement elem = ((OIdentifiable) source).getRecord();
      if (!elem.isVertex()) {
        throw new IllegalArgumentException("The sourceVertex must be a vertex record");
      }
      ctx.sourceVertex = elem.asVertex().get();
    } else {
      throw new IllegalArgumentException("The sourceVertex must be a vertex record");
    }

    Object dest = iParams[1];
    if (OMultiValue.isMultiValue(dest)) {
      if (OMultiValue.getSize(dest) > 1)
        throw new IllegalArgumentException("Only one destinationVertex is allowed");
      dest = OMultiValue.getFirstValue(dest);
      if (dest instanceof OResult && ((OResult) dest).isElement()) {
        dest = ((OResult) dest).getElement().get();
      }
    }
    dest = OSQLHelper.getValue(dest, record, iContext);
    if (dest instanceof OIdentifiable) {
      OElement elem = ((OIdentifiable) dest).getRecord();
      if (!elem.isVertex()) {
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
    ArrayDeque<OVertex> nextLevelQueue = new ArrayDeque<OVertex>();
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
    ctx.queueLeft = nextLevelQueue;
    return null;
  }

  protected List<ORID> walkRight(final OSQLFunctionShortestPath.OShortestPathContext ctx) {
    final ArrayDeque<OVertex> nextLevelQueue = new ArrayDeque<OVertex>();

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