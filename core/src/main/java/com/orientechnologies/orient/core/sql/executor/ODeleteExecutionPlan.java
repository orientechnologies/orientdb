package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.command.OCommandContext;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class ODeleteExecutionPlan extends OUpdateExecutionPlan {

  public ODeleteExecutionPlan(OCommandContext ctx) {
    super(ctx);
  }

}

