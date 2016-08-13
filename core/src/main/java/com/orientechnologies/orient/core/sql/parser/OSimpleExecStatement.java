package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OSingleOpExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OTodoResultSet;

import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner.
 * All the execution is delegated to the statement itself, with the execute(ctx) method.
 *
 * @author Luigi Dell'Aquila
 */
public abstract class OSimpleExecStatement extends OStatement {

  public OSimpleExecStatement(int id) {
    super(id);
  }

  public OSimpleExecStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OTodoResultSet executeSimple(OCommandContext ctx);

  public OTodoResultSet execute(ODatabase db, Object[] args) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    ctx.setDatabase(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    OSingleOpExecutionPlan executionPlan = (OSingleOpExecutionPlan) createExecutionPlan(ctx);
    return executionPlan.executeInternal(ctx);
  }

  public OTodoResultSet execute(ODatabase db, Map params) {
    OBasicCommandContext ctx = new OBasicCommandContext();
    ctx.setDatabase(db);
    ctx.setInputParameters(params);
    OSingleOpExecutionPlan executionPlan = (OSingleOpExecutionPlan) createExecutionPlan(ctx);
    return executionPlan.executeInternal(ctx);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx) {
    return new OSingleOpExecutionPlan(ctx, this);
  }

}
