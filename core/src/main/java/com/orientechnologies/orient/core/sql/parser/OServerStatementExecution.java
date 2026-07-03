package com.orientechnologies.orient.core.sql.parser;

import com.orientechnologies.orient.core.command.OServerCommandContext;
import com.orientechnologies.orient.core.sql.executor.stream.OExecutionStream;
import java.util.Map;

public interface OServerStatementExecution {

  public abstract OExecutionStream executeSimple(OServerCommandContext ctx);

  public void toString(Map<Object, Object> params, StringBuilder builder);
}
