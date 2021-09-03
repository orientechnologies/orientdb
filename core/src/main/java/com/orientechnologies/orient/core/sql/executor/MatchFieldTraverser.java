package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.parser.OFieldMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;
import java.util.Collections;
import java.util.Iterator;

public class MatchFieldTraverser extends MatchEdgeTraverser {
  public MatchFieldTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  public MatchFieldTraverser(OResult lastUpstreamRecord, OMatchPathItem item) {
    super(lastUpstreamRecord, item);
  }

  protected Iterable<OResultInternal> traversePatternEdge(
      OIdentifiable startingPoint, OCommandContext iCommandContext) {

    Iterable possibleResults = null;
    if (this.item.getFilter() != null) {
      String alias = getEndpointAlias();
      Object matchedNodes =
          iCommandContext.getVariable(MatchPrefetchStep.PREFETCHED_MATCH_ALIAS_PREFIX + alias);
      if (matchedNodes != null) {
        if (matchedNodes instanceof Iterable) {
          possibleResults = (Iterable) matchedNodes;
        } else {
          possibleResults = Collections.singleton(matchedNodes);
        }
      }
    }

    Object prevCurrent = iCommandContext.getVariable("$current");
    iCommandContext.setVariable("$current", startingPoint);
    Object qR;
    try {
      // TODO check possible results!
      qR = ((OFieldMatchPathItem) this.item).getExp().execute(startingPoint, iCommandContext);
    } finally {
      iCommandContext.setVariable("$current", prevCurrent);
    }

    if (qR == null) {
      return Collections.EMPTY_LIST;
    }
    if (qR instanceof OIdentifiable) {
      return Collections.singleton(new OResultInternal((OIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      final Iterator<Object> iter = ((Iterable) qR).iterator();
      Iterable<OResultInternal> result =
          () ->
              new Iterator<OResultInternal>() {
                private OResultInternal nextElement;

                @Override
                public boolean hasNext() {
                  if (nextElement == null) {
                    fetchNext();
                  }
                  return nextElement != null;
                }

                @Override
                public OResultInternal next() {
                  if (nextElement == null) {
                    fetchNext();
                  }
                  if (nextElement == null) {
                    throw new IllegalStateException();
                  }
                  OResultInternal res = nextElement;
                  nextElement = null;
                  return res;
                }

                public void fetchNext() {
                  while (iter.hasNext()) {
                    Object o = iter.next();
                    if (o instanceof OIdentifiable) {
                      nextElement = new OResultInternal((OIdentifiable) o);
                      break;
                    } else if (o instanceof OResultInternal) {
                      nextElement = (OResultInternal) o;
                      break;
                    } else if (o == null) {
                      continue;
                    } else {
                      throw new UnsupportedOperationException();
                    }
                  }
                }
              };

      return result;
    }
    return Collections.EMPTY_LIST;
  }
}
