package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Map;


public interface OScriptInterceptor {

  void preExecute(ODatabaseDocumentInternal database, String language, String script, Object params);

}
