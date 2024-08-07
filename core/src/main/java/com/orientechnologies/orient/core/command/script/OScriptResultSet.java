package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.sql.executor.OExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSetInternal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

/**
 * Wrapper of OIteratorResultSet Used in script results with conversion to OResult for single
 * iteration Created by Enrico Risa on 27/01/17.
 */
public class OScriptResultSet implements OResultSetInternal {

  protected final Iterator iterator;
  protected final OScriptTransformer transformer;

  public OScriptResultSet(Iterator iter, OScriptTransformer transformer) {
    this.iterator = iter;
    this.transformer = transformer;
  }

  @Override
  public boolean hasNext() {
    return iterator.hasNext();
  }

  @Override
  public void close() {}

  @Override
  public Optional<OExecutionPlan> getExecutionPlan() {
    return Optional.empty();
  }

  @Override
  public Map<String, Long> getQueryStats() {
    return new HashMap<>();
  }

  @Override
  public OResult next() {
    Object next = iterator.next();
    return transformer.toResult(next);
  }

  @Override
  public boolean isDetached() {
    return false;
  }

  @Override
  public boolean isExplain() {
    return false;
  }
}
