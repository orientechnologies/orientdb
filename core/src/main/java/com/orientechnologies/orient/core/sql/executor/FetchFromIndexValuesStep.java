package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/** Created by luigidellaquila on 02/08/16. */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  public FetchFromIndexValuesStep(
      IndexSearchDescriptor desc, boolean orderAsc, OCommandContext ctx, boolean profilingEnabled) {
    super(desc, orderAsc, ctx, profilingEnabled);
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
  public boolean canBeCached() {
    return false;
  }
}
