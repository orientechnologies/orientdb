package com.orientechnologies.orient.core.sql.executor.stream;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.Collection;
import java.util.Iterator;

public class OResultCollectionExecutionStream implements OExecutionStream {

  private Iterator<OResult> iterator;

  public OResultCollectionExecutionStream(Collection<OResult> data) {
    iterator = data.iterator();
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

  @Override
  public boolean isFullInMemory(OCommandContext ctx) {
    return true;
  }

  @Override
  public boolean isTermination(OCommandContext ctx) {
    return false;
  }
}
