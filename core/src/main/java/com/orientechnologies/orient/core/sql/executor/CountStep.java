package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * Counts the records from the previous steps.
 * Returns a record with a single property, called "count" containing the count of records received from pervious steps
 *
 * @author Luigi Dell'Aquila
 */
public class CountStep extends AbstractExecutionStep {

  boolean executed = false;

  public CountStep(OCommandContext ctx) {
    super(ctx);
  }

  @Override public OTodoResultSet syncPull(OCommandContext ctx, int nRecords) throws OTimeoutException {
    if (executed) {
      return new OInternalResultSet();
    }
    OResultInternal resultRecord = new OResultInternal();
    executed = true;
    long count = 0;
    while (true) {
      OTodoResultSet prevResult = getPrev().get().syncPull(ctx, nRecords);
      if (!prevResult.hasNext()) {
        OInternalResultSet result = new OInternalResultSet();
        result.add(resultRecord);
        return result;
      }
      while (prevResult.hasNext()) {
        prevResult.next();
        resultRecord.setProperty("count", ++count);
      }
    }
  }

  @Override public void asyncPull(OCommandContext ctx, int nRecords, OExecutionCallback callback) throws OTimeoutException {

  }

  @Override public void sendResult(Object o, Status status) {

  }


  @Override public String prettyPrint(int depth, int indent) {
    String spaces = OExecutionStepInternal.getIndent(depth, indent);
    StringBuilder result = new StringBuilder();
    result.append(spaces);
    result.append("+ COUNT");
    return result.toString();
  }
}
