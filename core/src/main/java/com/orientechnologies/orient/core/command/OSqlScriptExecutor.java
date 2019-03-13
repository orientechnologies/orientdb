package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.core.sql.executor.OInternalExecutionPlan;
import com.orientechnologies.orient.core.sql.executor.OInternalResultSet;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.executor.OScriptExecutionPlan;
import com.orientechnologies.orient.core.sql.parser.OLetStatement;
import com.orientechnologies.orient.core.sql.parser.OLocalResultSet;
import com.orientechnologies.orient.core.sql.parser.OStatement;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by tglman on 25/01/17.
 */
public class OSqlScriptExecutor implements OScriptExecutor {

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... args)
      throws OCommandSQLParsingException, OCommandExecutionException {

    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<OStatement> statements = OSQLEngine.parseScript(script, database);

    OCommandContext scriptContext = new OBasicCommandContext();
    ((OBasicCommandContext) scriptContext).setDatabase(database);
    Map<Object, Object> params = new HashMap<>();
    if (args != null) {
      for (int i = 0; i < args.length; i++) {
        params.put(i, args[i]);
      }
    }
    scriptContext.setInputParameters(params);

    OScriptExecutionPlan plan = new OScriptExecutionPlan(scriptContext);

    for (OStatement stm : statements) {
      if (stm.getOriginalStatement() == null) {
        stm.setOriginalStatement(stm.toString());
      }
      OInternalExecutionPlan sub = stm.createExecutionPlan(scriptContext);
      plan.chain(sub, false);
      if (stm instanceof OLetStatement) {
        scriptContext.declareScriptVariable(((OLetStatement) stm).getName().getStringValue());
      }
    }
    return new OLocalResultSet(plan);
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Map params) {
    if (!script.trim().endsWith(";")) {
      script += ";";
    }
    List<OStatement> statements = OSQLEngine.parseScript(script, database);
    OResultSet rs = null;
    OCommandContext scriptContext = new OBasicCommandContext();
    scriptContext.setInputParameters(params);
    for (OStatement stm : statements) {
      if (rs != null) {
        rs.close();
      }
      rs = stm.execute(database, params, scriptContext, false);
    }
    if (rs == null) {
      rs = new OInternalResultSet();
    }
    return rs;
  }
}
