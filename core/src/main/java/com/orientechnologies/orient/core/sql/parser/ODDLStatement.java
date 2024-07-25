package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.sql.executor.ODDLExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionResultSet;
import com.orientechnologies.orient.core.sql.executor.resultset.OExecutionStream;
import java.util.HashMap;
import java.util.Map;

/** Created by luigidellaquila on 12/08/16. */
public abstract class ODDLStatement extends OStatement {

  public ODDLStatement(int id) {
    super(id);
  }

  public ODDLStatement(OrientSql p, int id) {
    super(p, id);
  }

  public abstract OExecutionStream executeDDL(OCommandContext ctx);

  public OResultSet execute(
      ODatabaseSession db, Object[] args, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext(db);
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return new OExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public OResultSet execute(
      ODatabaseSession db, Map params, OCommandContext parentCtx, boolean usePlanCache) {
    OBasicCommandContext ctx = new OBasicCommandContext(db);
    if (parentCtx != null) {
      ctx.setParentWithoutOverridingChild(parentCtx);
    }
    ctx.setInputParameters(params);
    ODDLExecutionPlan executionPlan = (ODDLExecutionPlan) createExecutionPlan(ctx, false);
    return new OExecutionResultSet(executionPlan.executeInternal(ctx), ctx, executionPlan);
  }

  public OInternalExecutionPlan createExecutionPlan(OCommandContext ctx, boolean enableProfiling) {
    return new ODDLExecutionPlan(this);
  }
}
