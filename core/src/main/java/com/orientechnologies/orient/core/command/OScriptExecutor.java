package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Map;

/** Created by tglman on 25/01/17. */
public interface OScriptExecutor {

  OResultSet execute(ODatabaseDocumentInternal database, String script, Object... params);

  OResultSet execute(ODatabaseDocumentInternal database, String script, Map params);

  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs);

  void registerInterceptor(OScriptInterceptor interceptor);

  void unregisterInterceptor(OScriptInterceptor interceptor);

  default void close(String iDatabaseName) {}

  default void closeAll() {}
}
