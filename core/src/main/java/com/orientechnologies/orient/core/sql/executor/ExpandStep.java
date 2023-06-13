package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.sql.executor.resultset.OIteratorResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OSubResultsResultSet;
import java.util.Collections;
import java.util.Iterator;

/**
 * Expands a result-set. The pre-requisite is that the input element contains only one field (no
 * matter the name)
 */
public class ExpandStep extends AbstractExecutionStep {

  private long cost = 0;

  public ExpandStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx) throws OTimeoutException {
    if (prev == null || !prev.isPresent()) {
      throw new OCommandExecutionException("Cannot expand without a target");
    }
    OResultSet resultSet = getPrev().get().syncPull(ctx);

    OResultSet result =
        new OSubResultsResultSet(
            new Iterator<OResultSet>() {
              private OResultSet next;

              @Override
              public boolean hasNext() {
                fetchNext();
                return next != null;
              }

              private void fetchNext() {
                if (next == null) {
                  next = nextSequence(ctx, resultSet);
                }
              }

              @Override
              public OResultSet next() {
                if (!hasNext()) {
                  throw new IllegalStateException();
                }
                OResultSet n = next;
                this.next = null;
                return n;
              }
            });
    return result;
  }

  public OResultSet nextSequence(OCommandContext ctx, OResultSet resultSet) {
    while (resultSet.hasNext()) {
      OResult nextAggregateItem = resultSet.next();
      long begin = profilingEnabled ? System.nanoTime() : 0;
      try {
        if (nextAggregateItem.getPropertyNames().size() == 0) {
          continue;
        }
        if (nextAggregateItem.getPropertyNames().size() > 1) {
          throw new IllegalStateException("Invalid EXPAND on record " + nextAggregateItem);
        }

        String propName = nextAggregateItem.getPropertyNames().iterator().next();
        Object projValue = nextAggregateItem.getProperty(propName);
        if (projValue == null) {
          continue;
        }
        if (projValue instanceof OIdentifiable) {
          ORecord rec = ((OIdentifiable) projValue).getRecord();
          if (rec == null) {
            continue;
          }
          OResultInternal res = new OResultInternal(rec);

          return new OIteratorResultSet(Collections.singleton(res).iterator());
        } else if (projValue instanceof OResult) {
          return new OIteratorResultSet(Collections.singleton((OResult) projValue).iterator());
        } else if (projValue instanceof Iterator) {
          return new OIteratorResultSet((Iterator) projValue);
        } else if (projValue instanceof Iterable) {
          return new OIteratorResultSet(((Iterable) projValue).iterator());
        }
      } finally {
        if (profilingEnabled) {
          cost += (System.nanoTime() - begin);
        }
      }
    }
    return null;
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

  @Override
  public long getCost() {
    return cost;
  }
}
