package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OAdminCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Map;

public interface OAdminStatementExecution {

  public abstract OExecutionStream executeSimple(OAdminCommandContext ctx);

  public void toString(Map<Object, Object> params, StringBuilder builder);
}
