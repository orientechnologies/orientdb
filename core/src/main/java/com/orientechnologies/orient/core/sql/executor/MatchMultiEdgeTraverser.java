package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.*;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * Created by luigidellaquila on 14/10/16.
 */
public class MatchMultiEdgeTraverser extends MatchEdgeTraverser {
  public MatchMultiEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected Iterable<OIdentifiable> traversePatternEdge(OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    //    if (this.edge.edge.item.getFilter() != null) {
    //      String alias = this.edge.edge.item.getFilter().getAlias();
    //      Object matchedNodes = iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
    //      if (matchedNodes != null) {
    //        if (matchedNodes instanceof Iterable) {
    //          possibleResults = (Iterable) matchedNodes;
    //        } else {
    //          possibleResults = Collections.singleton(matchedNodes);
    //        }
    //      }
    //    }

    OMultiMatchPathItem item = (OMultiMatchPathItem) this.item;
    List<Object> result = new ArrayList<>();

    List<Object> nextStep = new ArrayList<>();
    nextStep.add(startingPoint);

    Object oldCurrent = iCommandContext.getVariable("$current");
    for (OMatchPathItem sub : item.getItems()) {
      List<Object> rightSide = new ArrayList<>();
      for (Object o : nextStep) {
        OWhereClause whileCond = sub.getFilter() == null ? null : sub.getFilter().getWhileCondition();

        OMethodCall method = sub.getMethod();
        if (sub instanceof OMatchPathItemFirst) {
          method = ((OMatchPathItemFirst) sub).getFunction().toMethod();
        }

        if (whileCond != null) {
          Object current = o;
          if (current instanceof OResult) {
            current = ((OResult) current).getElement();
          }
          MatchEdgeTraverser subtraverser = new MatchEdgeTraverser(null, sub);
          subtraverser.executeTraversal(iCommandContext, sub, (OIdentifiable) current, 0).forEach(x -> rightSide.add(x));

        } else {
          iCommandContext.setVariable("$current", o);
          Object nextSteps = method.execute(o, possibleResults, iCommandContext);
          if (nextSteps instanceof Collection) {
            rightSide.addAll((Collection<?>) nextSteps);
          } else if (nextSteps instanceof OIdentifiable || nextSteps instanceof OResult) {
            rightSide.add(nextSteps);
          } else if (nextSteps instanceof Iterable) {
            ((Iterable) nextSteps).forEach(x -> rightSide.add(x));
          } else if (nextSteps instanceof Iterator) {
            ((Iterator) nextSteps).forEachRemaining(x -> rightSide.add(x));
          }
        }
      }
      nextStep = rightSide;
      result = rightSide;
    }

    iCommandContext.setVariable("$current", oldCurrent);
    //    return (qR instanceof Iterable) ? (Iterable) qR : Collections.singleton((OIdentifiable) qR);
    return (Iterable) result;
  }
}
