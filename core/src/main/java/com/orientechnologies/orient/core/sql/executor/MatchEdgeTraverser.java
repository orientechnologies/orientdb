package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 23/09/16.
 */
public class MatchEdgeTraverser {
  private final OResult       sourceRecord;
  private final EdgeTraversal edge;

  Iterator<OIdentifiable> downstream;

  public MatchEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
  }

  public boolean hasNext(OCommandContext ctx) {
    init(ctx);
    return downstream.hasNext();
  }

  public OResult next(OCommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext()) {
      throw new IllegalStateException();
    }
    OIdentifiable nextElement = downstream.next();
    OResultInternal result = new OResultInternal();
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(this.edge.edge.item.getFilter().getAlias(), nextElement);
    return result;
  }

  private void init(OCommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(edge.edge.out.alias);
      if(startingElem instanceof OResult){
        startingElem = ((OResult) startingElem).getElement();
      }
      downstream = executeTraversal(ctx, (OIdentifiable) startingElem, 0).iterator();
    }
  }

  private Iterable<OIdentifiable> executeTraversal(OCommandContext iCommandContext, OIdentifiable startingPoint, int depth) {

    OWhereClause filter = null;
    OWhereClause whileCondition = null;
    Integer maxDepth = null;
    if (this.edge.edge.item.getFilter() != null) {
      filter = this.edge.edge.item.getFilter().getFilter();
      whileCondition = this.edge.edge.item.getFilter().getWhileCondition();
      maxDepth = this.edge.edge.item.getFilter().getMaxDepth();
    }

    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    if (whileCondition == null && maxDepth == null) {// in this case starting point is not returned and only one level depth is
      // evaluated
      Iterable<OIdentifiable> queryResult = traversePatternEdge(startingPoint, iCommandContext);

      if (this.edge.edge.item.getFilter() == null || this.edge.edge.item.getFilter().getFilter() == null) {
        return queryResult;
      }

      for (OIdentifiable origin : queryResult) {
        Object previousMatch = iCommandContext.getVariable("$currentMatch");
        iCommandContext.setVariable("$currentMatch", origin);
        if (filter == null || filter.matchesFilters(origin, iCommandContext)) {
          result.add(origin);
        }
        iCommandContext.setVariable("$currentMatch", previousMatch);
      }
    } else {// in this case also zero level (starting point) is considered and traversal depth is given by the while condition
      iCommandContext.setVariable("$depth", depth);
      Object previousMatch = iCommandContext.getVariable("$currentMatch");
      iCommandContext.setVariable("$currentMatch", startingPoint);
      if (filter == null || filter.matchesFilters(startingPoint, iCommandContext)) {
        result.add(startingPoint);
      }

      if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition
          .matchesFilters(startingPoint, iCommandContext))) {

        Iterable<OIdentifiable> queryResult = traversePatternEdge(startingPoint, iCommandContext);

        for (OIdentifiable origin : queryResult) {
          // TODO consider break strategies (eg. re-traverse nodes)
          Iterable<OIdentifiable> subResult = executeTraversal(iCommandContext, origin, depth + 1);
          if (subResult instanceof Collection) {
            result.addAll((Collection<? extends OIdentifiable>) subResult);
          } else {
            for (OIdentifiable i : subResult) {
              result.add(i);
            }
          }
        }
      }
      iCommandContext.setVariable("$currentMatch", previousMatch);
    }
    return result;
  }

  private Iterable<OIdentifiable> traversePatternEdge(OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.edge.edge.item.getFilter() != null) {
      String alias = this.edge.edge.item.getFilter().getAlias();
      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object qR = this.edge.edge.item.getMethod().execute(startingPoint, possibleResults, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable) qR);
  }

}
