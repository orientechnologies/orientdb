package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Iterator;

public class OResultIteratorExecutionStream implements OExecutionStream {

  private Iterator<OResult> iterator;

  public OResultIteratorExecutionStream(Iterator<OResult> iterator) {
    this.iterator = iterator;
  }

  @Override
  public boolean hasNext(OCommandContext ctx) {
    return iterator.hasNext();
  }

  @Override
  public OResult next(OCommandContext ctx) {
    return iterator.next();
  }

  @Override
  public void close(OCommandContext ctx) {}
}
