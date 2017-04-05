package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 08/07/16.
 */
public class EmptyDataGeneratorStep extends AbstractExecutionStep {

  private long cost = 0;

  int size;
  int served = 0;

  public EmptyDataGeneratorStep(int size, OCommandContext ctx) {
    super(ctx);
    this.size = size;
  }

  @Override
  public OResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    return new OResultSet() {
      @Override
      public boolean hasNext() {
        return served < size;
      }

      @Override
      public OResult next() {
        long begin = System.nanoTime();
        try {

          if (served < size) {
            served++;
            OResultInternal result = new OResultInternal();
            ctx.setVariable("$current", result);
            return result;
          }
          throw new IllegalStateException();
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
        served = 0;
      }
    };
  }

  @Override
  public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override
  public void sendResult(Object o, Status status) {

  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ GENERATE " + size + " EMPTY " + (size == 1 ? "RECORD" : "RECORDS");
  }

  @Override
  public long getCost() {
    return cost;
  }
}
