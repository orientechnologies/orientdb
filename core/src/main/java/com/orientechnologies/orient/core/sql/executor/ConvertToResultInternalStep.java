package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.concur.OTimeoutException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;

/**
 * takes a result set made of OUpdatableRecord instances and transforms it in another result set
 * made of normal OResultInternal instances.
 *
 * <p>This is the opposite of ConvertToUpdatableResultStep
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ConvertToResultInternalStep extends AbstractExecutionStep {

  public ConvertToResultInternalStep(OCommandContext ctx, boolean profilingEnabled) {
    super(ctx, profilingEnabled);
  }

  @Override
  public OExecutionStream syncPull(OCommandContext ctx) throws OTimeoutException {
    if (!prev.isPresent()) {
      throw new IllegalStateException("filter step requires a previous step");
    }
    OExecutionStream resultSet = prev.get().syncPull(ctx);
    return attachProfile(resultSet.filter(this::filterMap));
  }

  private OResult filterMap(OResult result, OCommandContext ctx) {
    if (result instanceof OUpdatableResult) {
      ORecord element = result.getElement().get().getRecord();
      if (element != null && element instanceof ODocument) {
        return new OResultInternal(element);
      }
      return result;
    }
    return null;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    String result =
        OExecutionStepInternal.getIndent(depth, indent) + "+ CONVERT TO REGULAR RESULT ITEM";
    if (profilingEnabled) {
      result += " (" + getCostFormatted() + ")";
    }
    return result;
  }
}
