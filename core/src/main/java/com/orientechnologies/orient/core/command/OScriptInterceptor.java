package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;

public interface OScriptInterceptor {

  void preExecute(
      ODatabaseDocumentInternal database, String language, String script, Object params);
}
