package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Iterator;

public final class OLoaderExecutionStream implements OExecutionStream {
  private OResult nextResult = null;
  private final Iterator<OIdentifiable> iterator;

  public OLoaderExecutionStream(Iterator<OIdentifiable> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    if (nextResult == null) {
      fetchNext(ctx);
    }
    return nextResult != null;
  }

  @Override
  public OResult next(OCommandContext ctx) {
    if (!hasNext(ctx)) {
      throw new IllegalStateException();
    }

    OResult result = nextResult;
    nextResult = null;
    ctx.setVariable("$current", result.toElement());
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {}

  private void fetchNext(OCommandContext ctx) {
    if (nextResult != null) {
      return;
    }
    while (iterator.hasNext()) {
      OIdentifiable nextRid = iterator.next();
      if (nextRid != null) {
        if (nextRid instanceof ORecord) {
          nextResult = new OResultInternal(nextRid);
          return;
        } else {
          OIdentifiable nextDoc = (OIdentifiable) ctx.getDatabase().load(nextRid.getIdentity());
          if (nextDoc != null) {
            nextResult = new OResultInternal(nextDoc);
            return;
          }
        }
      }
    }
    return;
  }
}
