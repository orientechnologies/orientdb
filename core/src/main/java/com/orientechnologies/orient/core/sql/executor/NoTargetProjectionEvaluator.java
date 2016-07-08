package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.parser.OProjection;

/**
 * Created by luigidellaquila on 06/07/16.
 */
public class NoTargetProjectionEvaluator implements OExecutionStep {
  //execution pipeline
  private OExecutionStep next;

  private boolean keepRunning = true;

  //parser tree
  private final OProjection projection;

  //data context
  private OTodoResultSet calculatedResult;

  public NoTargetProjectionEvaluator(OProjection projection, OCommandContext ctx) {
    super();
    this.projection = projection;
  }

  private OTodoResultSet calculate(OProjection projection, OCommandContext ctx) {

    Object result = projection.calculate(ctx, null);
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
      return null;
    }
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
    this.keepRunning = false;
  }

  @Override public void setPrevious(OExecutionStep step) {
    throw new UnsupportedOperationException("a " + getClass().getSimpleName() + " cannot have a previous step");
  }


  @Override public void setNext(OExecutionStep step) {
    this.next = step;
  }

  @Override public void close() {
    this.keepRunning = false;
    if(calculatedResult!=null){
      this.calculatedResult.close();
    }
  }

  @Override public void sendResult(Object o, Status status) {
    //Do nothing, nobody will send results to this step
  }

}
