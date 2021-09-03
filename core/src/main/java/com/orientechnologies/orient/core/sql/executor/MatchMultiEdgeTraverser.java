package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.parser.OMatchFilter;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItemFirst;
import com.orientechnologies.orient.core.sql.parser.OMethodCall;
import com.orientechnologies.orient.core.sql.parser.OMultiMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OWhereClause;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

/** Created by luigidellaquila on 14/10/16. */
public class MatchMultiEdgeTraverser extends MatchEdgeTraverser {
  public MatchMultiEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected Iterable<OResultInternal> traversePatternEdge(
      OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    //    if (this.edge.edge.item.getFilter() != null) {
    //      String alias = this.edge.edge.item.getFilter().getAlias();
    //      Object matchedNodes =
    // iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    //      if (matchedNodes != null) {
    //        if (matchedNodes instanceof Iterable) {
    //          possibleResults = (Iterable) matchedNodes;
    //        } else {
    //          possibleResults = Collections.singleton(matchedNodes);
    //        }
    //      }
    //    }

    OMultiMatchPathItem item = (OMultiMatchPathItem) this.item;
    List<OResultInternal> result = new ArrayList<>();

    List<Object> nextStep = new ArrayList<>();
    nextStep.add(startingPoint);

    Object oldCurrent = iCommandContext.getVariable("$current");
    for (OMatchPathItem sub : item.getItems()) {
      List<OResultInternal> rightSide = new ArrayList<>();
      for (Object o : nextStep) {
        OWhereClause whileCond =
            sub.getFilter() == null ? null : sub.getFilter().getWhileCondition();

        OMethodCall method = sub.getMethod();
        if (sub instanceof OMatchPathItemFirst) {
          method = ((OMatchPathItemFirst) sub).getFunction().toMethod();
        }

        if (whileCond != null) {
          Object current = o;
          if (current instanceof OResult) {
            current = ((OResult) current).getElement().orElse(null);
          }
          MatchEdgeTraverser subtraverser = new MatchEdgeTraverser(null, sub);
          subtraverser
              .executeTraversal(iCommandContext, sub, (OIdentifiable) current, 0, null)
              .forEach(x -> rightSide.add(x));

        } else {
          iCommandContext.setVariable("$current", o);
          Object nextSteps = method.execute(o, possibleResults, iCommandContext);
          if (nextSteps instanceof Collection) {
            ((Collection) nextSteps)
                .stream()
                    .map(x -> toOResultInternal(x))
                    .filter(Objects::nonNull)
                    .filter(
                        x ->
                            matchesCondition((OResultInternal) x, sub.getFilter(), iCommandContext))
                    .forEach(i -> rightSide.add((OResultInternal) i));
          } else if (nextSteps instanceof OIdentifiable) {
            OResultInternal res = new OResultInternal((OIdentifiable) nextSteps);
            if (matchesCondition(res, sub.getFilter(), iCommandContext)) {
              rightSide.add(res);
            }
          } else if (nextSteps instanceof OResultInternal) {
            if (matchesCondition((OResultInternal) nextSteps, sub.getFilter(), iCommandContext)) {
              rightSide.add((OResultInternal) nextSteps);
            }
          } else if (nextSteps instanceof Iterable) {
            for (Object step : (Iterable) nextSteps) {
              OResultInternal converted = toOResultInternal(step);
              if (converted != null
                  && matchesCondition(converted, sub.getFilter(), iCommandContext)) {
                rightSide.add(converted);
              }
            }
          } else if (nextSteps instanceof Iterator) {
            Iterator iterator = (Iterator) nextSteps;
            while (iterator.hasNext()) {
              OResultInternal converted = toOResultInternal(iterator.next());
              if (converted != null
                  && matchesCondition(converted, sub.getFilter(), iCommandContext)) {
                rightSide.add(converted);
              }
            }
          }
        }
      }
      nextStep = (List) rightSide;
      result = rightSide;
    }

    iCommandContext.setVariable("$current", oldCurrent);
    //    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable)
    // qR);
    return (Iterable) result;
  }

  private boolean matchesCondition(OResultInternal x, OMatchFilter filter, OCommandContext ctx) {
    if (filter == null) {
      return true;
    }
    OWhereClause where = filter.getFilter();
    if (where == null) {
      return true;
    }
    return where.matchesFilters(x, ctx);
  }

  private OResultInternal toOResultInternal(Object x) {
    if (x instanceof OResultInternal) {
      return (OResultInternal) x;
    }
    if (x instanceof OIdentifiable) {
      return new OResultInternal((OIdentifiable) x);
    }
    throw new OCommandExecutionException("Cannot execute traversal on " + x);
  }
}
