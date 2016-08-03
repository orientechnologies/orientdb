package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OProjection;

/**
 * @author Luigi Dell'Aquila
 */
public class ProjectionCalculationNoTargetStep extends AbstractExecutionStep {

  private boolean keepRunning = true;

  //parser tree
  private final OProjection projection;

  //data context
  private OTodoResultSet calculatedResult;

  public ProjectionCalculationNoTargetStep(OProjection projection, OCommandContext ctx) {
    super(ctx);
    this.projection = projection;
  }

  private OTodoResultSet calculate(OProjection projection, OCommandContext ctx) {
    Object result = projection.calculateSingle(ctx, null);
    if (result instanceof OTodoResultSet) {
      return (OTodoResultSet) result;
    }
    if (result instanceof OResult) {
      OInternalResultSet rs = new OInternalResultSet();
      rs.add((OResult) result);
      return rs;
    }
    if (result instanceof Iterable) {
      return new OIteratorResultSet(((Iterable) result).iterator());
    }
    return new OInternalResultSet();
  }

  @Override public synchronized OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (!keepRunning) {
      return new OInternalResultSet();
    }
    getPrev().ifPresent(x -> x.syncPull(ctx, nRecords));
    if (calculatedResult == null) {
      calculatedResult = calculate(projection, ctx);
    }
    if (nRecords < 0) {
      keepRunning = false;
      return calculatedResult;
    }

    OInternalResultSet result = new OInternalResultSet();

    for (int i = 0; i < nRecords || nRecords < 0; i++) {
      if (!keepRunning) {
        return result;
      }
      if (calculatedResult.hasNext()) {
        OResult nextResult = calculatedResult.next();
        result.add(nextResult);
      } else {
        keepRunning = false;
        break;
      }
    }

    return result;
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {
    throw new UnsupportedOperationException();
  }

  @Override public void sendTimeout() {
    super.sendTimeout();
    this.keepRunning = false;
  }

  @Override public void close() {
    this.keepRunning = false;
    if (calculatedResult != null) {
      this.calculatedResult.close();
    }
    getPrev().ifPresent(p -> p.close());
  }

  @Override public void sendResult(Object o, Status status) {
    //do nothig
  }

  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    return spaces + "+ CALCULATE PROJECTIONS (no target)\n" + spaces + "  " + projection.toString() + "";
  }
}
