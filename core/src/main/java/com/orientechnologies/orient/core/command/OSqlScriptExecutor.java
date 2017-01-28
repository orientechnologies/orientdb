package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import java.util.Map;

/**
 * Created by tglman on 25/01/17.
 */
public class OSqlScriptExecutor implements OScriptExecutor {

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... params) {
    return database.command(script, params);
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Map params) {
    return database.command(script, params);
  }
}
