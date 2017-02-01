package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import javax.script.*;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 25/01/17.
 */
public class OJsr223ScriptExecutor implements OScriptExecutor {
  private String language;

  public OJsr223ScriptExecutor(String language) {
    this.language = language;
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... params) {
    Map<Integer, Object> par = new HashMap<>();

    for (int i = 0; i < params.length; i++) {
      par.put(i, params[i]);
    }
    return execute(database, script, par);
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Map params) {

    final OScriptManager scriptManager = Orient.instance().getScriptManager();
    CompiledScript compiledScript = null;

    final OPartitionedObjectPool.PoolEntry<ScriptEngine> entry = scriptManager.acquireDatabaseEngine(database.getName(), language);
    final ScriptEngine scriptEngine = entry.object;
    try {

      if (!(scriptEngine instanceof Compilable))
        throw new OCommandExecutionException("Language '" + language + "' does not support compilation");

      final Compilable c = (Compilable) scriptEngine;
      try {
        compiledScript = c.compile(script);
      } catch (ScriptException e) {
        scriptManager.throwErrorMessage(e, script);
      }

      final Bindings binding = scriptManager
          .bindContextVariables(compiledScript.getEngine(),compiledScript.getEngine().getBindings(ScriptContext.ENGINE_SCOPE), database, null, params);

      try {
        final Object ob = compiledScript.eval(binding);
        return scriptManager.getTransformer().toResultSet(ob);
      } catch (ScriptException e) {
        throw OException
            .wrapException(new OCommandScriptException("Error on execution of the script", script, e.getColumnNumber()), e);

      } finally {
        scriptManager.unbind(scriptEngine,binding, null, params);
      }
    } finally {
      scriptManager.releaseDatabaseEngine(language, database.getName(), entry);
    }
  }
}
