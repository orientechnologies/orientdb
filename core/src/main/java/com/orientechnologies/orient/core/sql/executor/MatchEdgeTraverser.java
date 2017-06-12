package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;

import java.util.*;

/**
 * Created by luigidellaquila on 23/09/16.
 */
public class MatchEdgeTraverser {
  protected OResult        sourceRecord;
  protected EdgeTraversal  edge;
  protected OMatchPathItem item;

  Iterator<OIdentifiable> downstream;

  public MatchEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    this.sourceRecord = lastUpstreamRecord;
    this.edge = edge;
    this.item = edge.edge.item;
  }

  public MatchEdgeTraverser(OResult lastUpstreamRecord, OMatchPathItem item) {
    this.sourceRecord = lastUpstreamRecord;
    this.item = item;
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
    String endPointAlias = getEndpointAlias();
    OIdentifiable nextElement = downstream.next();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    if (prevValue != null && !equals(prevValue, nextElement)) {
      return null;
    }
    OResultInternal result = new OResultInternal();
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, toResult(nextElement));
    return result;
  }

  protected boolean equals(Object prevValue, OIdentifiable nextElement) {
    if (prevValue instanceof OResult) {
      prevValue = ((OResult) prevValue).getElement().orElse(null);
    }
    if (nextElement instanceof OResult) {
      nextElement = ((OResult) nextElement).getElement().orElse(null);
    }
    return prevValue != null && prevValue.equals(nextElement);
  }

  protected Object toResult(OIdentifiable nextElement) {
    OResultInternal result = new OResultInternal();
    result.setElement(nextElement);
    return result;
  }

  protected String getStartingPointAlias() {
    return this.edge.edge.out.alias;
  }

  protected String getEndpointAlias() {
    if (this.item != null) {
      return this.item.getFilter().getAlias();
    }
    return this.edge.edge.in.alias;
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      Object startingElem = sourceRecord.getProperty(getStartingPointAlias());
      if (startingElem instanceof OResult) {
        startingElem = ((OResult) startingElem).getElement().orElse(null);
      }
      downstream = executeTraversal(ctx, this.item, (OIdentifiable) startingElem, 0).iterator();
    }
  }

  protected Iterable<OIdentifiable> executeTraversal(OCommandContext iCommandContext, OMatchPathItem item,
      OIdentifiable startingPoint, int depth) {

    OWhereClause filter = null;
    OWhereClause whileCondition = null;
    Integer maxDepth = null;
    String className = null;
    if (item.getFilter() != null) {
      filter = getTargetFilter(item);
      whileCondition = item.getFilter().getWhileCondition();
      maxDepth = item.getFilter().getMaxDepth();
      className = targetClassName(item, iCommandContext);
    }

    Set<OIdentifiable> result = new HashSet<OIdentifiable>();

    if (whileCondition == null && maxDepth == null) {// in this case starting point is not returned and only one level depth is
      // evaluated
      Iterable<OIdentifiable> queryResult = traversePatternEdge(startingPoint, iCommandContext);

      for (OIdentifiable origin : queryResult) {
        Object previousMatch = iCommandContext.getVariable("$currentMatch");
        iCommandContext.setVariable("$currentMatch", origin);
        if (matchesFilters(iCommandContext, filter, origin) && matchesClass(iCommandContext, className, origin)) {
          result.add(origin);
        }
        iCommandContext.setVariable("$currentMatch", previousMatch);
      }
    } else {// in this case also zero level (starting point) is considered and traversal depth is given by the while condition
      iCommandContext.setVariable("$depth", depth);
      Object previousMatch = iCommandContext.getVariable("$currentMatch");
      iCommandContext.setVariable("$currentMatch", startingPoint);
      if (matchesFilters(iCommandContext, filter, startingPoint) && matchesClass(iCommandContext, className, startingPoint)) {
        result.add(startingPoint);
      }

      if ((maxDepth == null || depth < maxDepth) && (whileCondition == null || whileCondition
          .matchesFilters(startingPoint, iCommandContext))) {

        Iterable<OIdentifiable> queryResult = traversePatternEdge(startingPoint, iCommandContext);

        for (OIdentifiable origin : queryResult) {
          //          if(origin.equals(startingPoint)){
          //            continue;
          //          }
          // TODO consider break strategies (eg. re-traverse nodes)
          Iterable<OIdentifiable> subResult = executeTraversal(iCommandContext, item, origin, depth + 1);
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

  protected OWhereClause getTargetFilter(OMatchPathItem item) {
    return item.getFilter().getFilter();
  }

  protected String targetClassName(OMatchPathItem item, OCommandContext iCommandContext) {
    return item.getFilter().getClassName(iCommandContext);
  }

  private boolean matchesClass(OCommandContext iCommandContext, String className, OIdentifiable origin) {
    if (className == null) {
      return true;
    }
    OElement element = null;
    if (origin instanceof OElement) {
      element = (OElement) origin;
    } else {
      Object record = origin.getRecord();
      if (record instanceof OElement) {
        element = (OElement) record;
      }
    }
    if (element != null) {
      Optional<OClass> clazz = element.getSchemaType();
      if (!clazz.isPresent()) {
        return false;
      }
      return clazz.get().isSubClassOf(className);
    }
    return false;
  }

  protected boolean matchesFilters(OCommandContext iCommandContext, OWhereClause filter, OIdentifiable origin) {
    return filter == null || filter.matchesFilters(origin, iCommandContext);
  }

  //TODO refactor this method to receive the item.

  protected Iterable<OIdentifiable> traversePatternEdge(OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      String alias = getEndpointAlias();
      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object qR = this.item.getMethod().execute(startingPoint, possibleResults, iCommandContext);
    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable) qR);
  }

}
