package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.parser.OIdentifier;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 17/03/17.
 */
public class CountFromClassStep extends AbstractExecutionStep {
  private final OIdentifier target;
  private final String      alias;

  private long cost = 0;

  private boolean executed = false;

  public CountFromClassStep(OIdentifier targetIndex, String alias, OCommandContext ctx) {
    super(ctx);
    this.target = targetIndex;
    this.alias = alias;
  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return !executed;
      }

      @Override
      public OResult next() {
        if (executed) {
          throw new IllegalStateException();
        }
        long begin = System.nanoTime();
        try {
          OClass clazz = ctx.getDatabase().getClass(target.getStringValue());
          long size = clazz.count();
          executed = true;
          OResultInternal result = new OResultInternal();
          result.setProperty(alias, size);
          return result;
        } finally {
          cost += (System.nanoTime() - begin);
        }
      }

      @Override
      public void close() {

      }

      @Override
      public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override
      public Map<String, Long> getQueryStats() {
        return null;
      }

      @Override
      public void reset() {
        CountFromClassStep.this.reset();
      }
    };
  }

  @Override
  public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override
  public void reset() {
    executed = false;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE CLASS SIZE: " + target;
  }

  @Override
  public long getCost() {
    return cost;
  }

}
