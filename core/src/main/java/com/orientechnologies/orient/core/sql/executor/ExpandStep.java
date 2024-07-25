package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.Iterator;

/**
 * Expands a result-set. The pre-requisite is that the input element contains only one field (no
 * matter the name)
 */
public class ExpandStep extends AbstractExecutionStep {

  public ExpandStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream internalStart(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OExecutionStream resultSet = getPrev().get().start(ctx);
    return resultSet.flatMap(this::nextResults);
  }

  private OExecutionStream nextResults(OResult nextAggregateItem, OCommandContext ctx) {
    if (nextAggregateItem.getPropertyNames().size() == 0) {
      return OExecutionStream.empty();
    }
    if (nextAggregateItem.getPropertyNames().size() > 1) {
      throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
    }

    String propName = nextAggregateItem.getPropertyNames().iterator().next();
    Object projValue = nextAggregateItem.getProperty(propName);
    return valueStream(projValue, ctx);
  }

  private OExecutionStream valueStream(Object projValue, OCommandContext ctx) {

    if (projValue == null) {
      return OExecutionStream.empty();
    }
    if (projValue instanceof OIdentifiable) {
      ORecord rec = ((OIdentifiable) projValue).getRecord();
      if (rec == null) {
        return OExecutionStream.empty();
      }
      OResultInternal res = new OResultInternal(rec);

      return OExecutionStream.singleton((OResult) res);
    } else if (projValue instanceof OResult) {
      return OExecutionStream.singleton((OResult) projValue);
    } else if (projValue instanceof Iterator) {
      return nestedExpand((Iterator) projValue);
    } else if (projValue instanceof Iterable) {
      return nestedExpand(((Iterable) projValue).iterator());
    } else {
      return OExecutionStream.empty();
    }
  }

  private OExecutionStream nestedExpand(final Iterator input) {
    return OExecutionStream.streamsFromIterator(input, this::valueStream);
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    String result = spaces + "+ EXPAND";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
