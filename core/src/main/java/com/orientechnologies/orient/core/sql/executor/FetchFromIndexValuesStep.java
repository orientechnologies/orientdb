package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.index.OIndex;

/**
 * Created by luigidellaquila on 02/08/16.
 */
public class FetchFromIndexValuesStep extends FetchFromIndexStep {

  private final boolean asc;

  public FetchFromIndexValuesStep(OIndex<?> index, boolean asc, OCommandContext ctx) {
    super(index, null, null, ctx);
    this.asc = asc;
  }

  @Override protected boolean isOrderAsc() {
    return asc;
  }

  @Override public String prettyPrint(int depth, int indent) {
    if(isOrderAsc()) {
      return OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VAUES ASC " + index.getName();
    }else{
      return OExecutionStepInternal.getIndent(depth, indent) + "+ FETCH FROM INDEX VAUES DESC " + index.getName();
    }
  }

}