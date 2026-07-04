package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OPrintContext;
import com.orientechnologies.orient.core.sql.executor.OSingleOpExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner. All the
 * execution is delegated to the statement itself, with the execute(ctx) method.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class OSimpleExecStatement extends OStatement {

  public OSimpleExecStatement(int id) {
    super(id);
  }

  public abstract OExecutionStream executeSimple(OCommandContext ctx);

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    var result = new OSingleOpExecutionPlan(this);
    result.setStatement(this.originalStatement);
    result.setGenericStatement(this.toGenericStatement());
    return result;
  }

  public String prettyPrint(OPrintContext ctx) {
    return toString();
  }
}
