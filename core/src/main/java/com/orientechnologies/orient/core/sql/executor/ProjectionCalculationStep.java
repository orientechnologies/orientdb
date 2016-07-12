package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OProjection;

import java.util.Map;
import java.util.Optional;

/**
 * Created by luigidellaquila on 12/07/16.
 */
public class ProjectionCalculationStep extends AbstractExecutionStep {
  private final OProjection projection;

  public ProjectionCalculationStep(OProjection projection, OCommandContext ctx) {
    super(ctx);
    this.projection = projection;
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("Cannot calculate projections without a previous source");
    }

    OTodoResultSet parentRs = prev.get().syncPull(ctx, nRecords);
    return new OTodoResultSet() {
      @Override public boolean hasNext() {
        return parentRs.hasNext();
      }

      @Override public OResult next() {
        return calculateProjections(ctx, parentRs.next());
      }

      @Override public void close() {
        parentRs.close();
      }

      @Override public Optional<OExecutionPlan> getExecutionPlan() {
        return null;
      }

      @Override public Map<String, Object> getQueryStats() {
        return null;
      }
    };
  }

  private OResult calculateProjections(OCommandContext ctx, OResult next) {
      return this.projection.calculateSingle(ctx, next);
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStep.getIndent(depth, indent);
    return spaces + "+ CALCULATE PROJECTIONS";
  }
}
