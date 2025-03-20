package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import com.orientechnologies.orient.core.sql.parser.OFieldMatchPathItem;
import com.orientechnologies.orient.core.sql.parser.OMatchPathItem;

public class MatchFieldTraverser extends MatchEdgeTraverser {
  public MatchFieldTraverser(OResult lastUpstreamRecord, EdgeTraversal edge) {
    super(lastUpstreamRecord, edge);
  }

  public MatchFieldTraverser(OResult lastUpstreamRecord, OMatchPathItem item) {
    super(lastUpstreamRecord, item);
  }

  protected OExecutionStream traversePatternEdge(
      OIdentifiable startingPoint, OCommandContext iCommandContext) {

    OResult prevCurrent = iCommandContext.getCurrent();
    iCommandContext.setCurrent(new OResultInternal(startingPoint));
    Object qR;
    try {
      // TODO check possible results!
      qR =
          ((OFieldMatchPathItem) this.item)
              .getExp()
              .execute(new OResultInternal(startingPoint), iCommandContext);
    } finally {
      iCommandContext.setCurrent(prevCurrent);
    }

    if (qR == null) {
      return OExecutionStream.empty();
    }
    if (qR instanceof OIdentifiable) {
      return OExecutionStream.singleton(new OResultInternal((OIdentifiable) qR));
    }
    if (qR instanceof Iterable) {
      return OExecutionStream.iterator(((Iterable) qR).iterator());
    }
    return OExecutionStream.empty();
  }
}
