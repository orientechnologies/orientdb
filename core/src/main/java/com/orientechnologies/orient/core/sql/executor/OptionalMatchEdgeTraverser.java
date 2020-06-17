package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import java.util.ArrayList;
import java.util.List;

/** Created by luigidellaquila on 17/10/16. */
public class OptionalMatchEdgeTraverser extends MatchEdgeTraverser {
  public static final OResult EMPTY_OPTIONAL = new OResultInternal();

  public OptionalMatchEdgeTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  protected void init(OCommandContext ctx) {
    if (downstream == null) {
      super.init(ctx);
      if (!downstream.hasNext()) {
        List x = new ArrayList();
        x.add(EMPTY_OPTIONAL);
        downstream = x.iterator();
      }
    }
  }

  public OResult next(OCommandContext ctx) {
    init(ctx);
    if (!downstream.hasNext()) {
      throw new IllegalStateException();
    }

    String endPointAlias = getEndpointAlias();
    Object prevValue = sourceRecord.getProperty(endPointAlias);
    OResultInternal next = downstream.next();

    if (isEmptyOptional(prevValue)) {
      return sourceRecord;
    }
    if (!isEmptyOptional(next)) {
      if (prevValue != null && !equals(prevValue, next.getElement().get())) {
        return null;
      }
    }

    OResultInternal result = new OResultInternal();
    for (String prop : sourceRecord.getPropertyNames()) {
      result.setProperty(prop, sourceRecord.getProperty(prop));
    }
    result.setProperty(endPointAlias, next.getElement().map(x -> toResult(x)).orElse(null));
    return result;
  }

  public static boolean isEmptyOptional(Object elem) {
    if (elem == EMPTY_OPTIONAL) {
      return true;
    }
    if (elem instanceof OResult && EMPTY_OPTIONAL == ((OResult) elem).getElement().orElse(null)) {
      return true;
    }

    return false;
  }
}
