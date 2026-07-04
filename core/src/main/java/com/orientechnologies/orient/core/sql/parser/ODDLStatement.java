package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.ODDLExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OPrintContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/** Created by luigidellaquila on 12/08/16. */
public abstract class ODDLStatement extends OStatement {

  public ODDLStatement(int id) {
    super(id);
  }

  public abstract OExecutionStream executeDDL(OCommandContext ctx);

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    var result = new ODDLExecutionPlan(this);
    result.setStatement(this.originalStatement);
    result.setGenericStatement(this.toGenericStatement());
    return result;
  }

  public String prettyPrint(OPrintContext ctx) {
    return toString();
  }
}
