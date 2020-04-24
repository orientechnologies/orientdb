package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicServerCommandContext;
import com.orientechnologies.orient.core.command.OServerCommandContext;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.OSingleOpServerExecutionPlan;
import java.util.HashMap;
import java.util.Map;

/**
 * Superclass for SQL statements that are too simple to deserve an execution planner. All the
 * execution is delegated to the statement itself, with the execute(ctx) method.
 *
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public abstract class OSimpleExecServerStatement extends OServerStatement {

  public OSimpleExecServerStatement(int id) {
    super(id);
  }

  public OSimpleExecServerStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OResultSet executeSimple(OServerCommandContext ctx);

  public OResultSet execute(
      OrientDBInternal db,
      Object[] args,
      OServerCommandContext parentContext,
      boolean usePlanCache) {
    OBasicServerCommandContext ctx = new OBasicServerCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setServer(db);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    OSingleOpServerExecutionPlan executionPlan =
        (OSingleOpServerExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OResultSet execute(
      OrientDBInternal db, Map params, OServerCommandContext parentContext, boolean usePlanCache) {
    OBasicServerCommandContext ctx = new OBasicServerCommandContext();
    if (parentContext != null) {
      ctx.setParentWithoutOverridingChild(parentContext);
    }
    ctx.setServer(db);
    ctx.setInputParameters(params);
    OSingleOpServerExecutionPlan executionPlan =
        (OSingleOpServerExecutionPlan) createExecutionPlan(ctx, false);
    return executionPlan.executeInternal(ctx);
  }

  public OInternalExecutionPlan createExecutionPlan(
      OServerCommandContext ctx, boolean enableProfiling) {
    return new OSingleOpServerExecutionPlan(ctx, this);
  }
}
