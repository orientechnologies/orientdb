package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.index.OIndex;

/** Created by luigidellaquila on 02/08/16. */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  private boolean asc;

  public FetchFromIndexValuesStep(
      OIndex index, boolean asc, OCommandContext ctx, boolean profilingEnabled) {
    super(index, null, null, ctx, profilingEnabled);
    this.asc = asc;
  }

  @Override
  protected boolean isOrderAsc() {
    return asc;
  }

  @Override
  public String prettyPrint(int depth, int indent) {
    if (isOrderAsc()) {
      return OExecutionStepInternal.getIndent(depth, indent)
          + "+ FETCH FROM INDEX VAUES ASC "
          + index.getName();
    } else {
      return OExecutionStepInternal.getIndent(depth, indent)
          + "+ FETCH FROM INDEX VAUES DESC "
          + index.getName();
    }
  }

  @Override
  public OResult serialize() {
    OResultInternal result = (OResultInternal) super.serialize();
    result.setProperty("asc", asc);
    return result;
  }

  @Override
  public void deserialize(OResult fromResult) {
    try {
      super.deserialize(fromResult);
      this.asc = fromResult.getProperty("asc");
    } catch (Exception e) {
      throw OException.wrapException(new OCommandExecutionException(""), e);
    }
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
