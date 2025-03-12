package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/** Created by luigidellaquila on 02/08/16. */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  public FetchFromIndexValuesStep(
      IndexSearchDescriptor desc, boolean orderAsc, OCommandContext ctx) {
    super(desc, orderAsc, ctx);
  }

  @Override
  public String prettyPrint(OPrintContext ctx) {
    if (isOrderAsc()) {
      return OExecutionStepInternal.getIndent(ctx)
          + "+ FETCH FROM INDEX VAUES ASC "
          + getIndexName();
    } else {
      return OExecutionStepInternal.getIndent(ctx)
          + "+ FETCH FROM INDEX VAUES DESC "
          + getIndexName();
    }
  }

  @Override
  public boolean canBeCached() {
    return false;
  }
}
