package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import java.util.Iterator;

public class OIteratorExecutionStream implements OExecutionStream {

  private Iterator<Object> iterator;

  public OIteratorExecutionStream(Iterator<Object> iter) {
    this.iterator = iter;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public OResult next(OCommandContext ctx) {
    Object val = iterator.next();
    if (val instanceof OResult) {
      return (OResult) val;
    }

    OResultInternal result;
    if (val instanceof OIdentifiable) {
      result = new OResultInternal((OIdentifiable) val);
    } else {
      result = new OResultInternal();
      result.setProperty("value", val);
    }
    return result;
  }

  @Override
  public void close(OCommandContext ctx) {}
}
