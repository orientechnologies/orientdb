package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

/** Created by tglman on 25/01/17. */
public class OJsr223ScriptExecutor extends OAbstractScriptExecutor {
  private final OScriptTransformer transformer;

  public OJsr223ScriptExecutor(String language, OScriptTransformer scriptTransformer) {
    super(language);
    this.language = language;
    this.transformer = scriptTransformer;
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Object... params) {

    preExecute(database, script, params);

    Map<Integer, Object> par = new HashMap<>();

    for (int i = 0; i < params.length; i++) {
      par.put(i, params[i]);
    }
    return execute(database, script, par);
  }

  @Override
  public OResultSet execute(ODatabaseDocumentInternal database, String script, Map params) {

    preExecute(database, script, params);

    final OScriptManager scriptManager =
        database.getSharedContext().getOrientDB().getScriptManager();
    CompiledScript compiledScript = null;

    final ScriptEngine scriptEngine =
        scriptManager.acquireDatabaseEngine(database.getName(), language);
    try {

      if (!(scriptEngine instanceof Compilable))
        throw new OCommandExecutionException(
            "Language '" + language + "' does not support compilation");

      final Compilable c = (Compilable) scriptEngine;
      try {
        compiledScript = c.compile(script);
      } catch (ScriptException e) {
        scriptManager.throwErrorMessage(e, script);
      }

      final Bindings binding =
          scriptManager.bindContextVariables(
              compiledScript.getEngine(),
              compiledScript.getEngine().getBindings(ScriptContext.ENGINE_SCOPE),
              database,
              null,
              params);

      try {
        final Object ob = compiledScript.eval(binding);
        return transformer.toResultSet(ob);
      } catch (ScriptException e) {
        throw OException.wrapException(
            new OCommandScriptException(
                "Error on execution of the script", script, e.getColumnNumber()),
            e);

      } finally {
        scriptManager.unbind(scriptEngine, binding, null, params);
      }
    } finally {
      scriptManager.releaseDatabaseEngine(language, database.getName(), scriptEngine);
    }
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();
    if (db == null) {
      db = ODatabaseRecordThreadLocal.instance().get();
    }
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(functionName);

    db.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, f.getName());

    final OScriptManager scriptManager = db.getSharedContext().getOrientDB().getScriptManager();

    final ScriptEngine scriptEngine =
        scriptManager.acquireDatabaseEngine(db.getName(), f.getLanguage());
    try {
      final Bindings binding =
          scriptManager.bind(
              scriptEngine,
              scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE),
              db,
              context,
              iArgs);

      try {
        final Object result;

        if (scriptEngine instanceof Invocable) {
          // INVOKE AS FUNCTION. PARAMS ARE PASSED BY POSITION
          final Invocable invocableEngine = (Invocable) scriptEngine;
          Object[] args = null;
          if (iArgs != null) {
            args = new Object[iArgs.size()];
            int i = 0;
            for (Entry<Object, Object> arg : iArgs.entrySet()) args[i++] = arg.getValue();
          } else {
            args = OCommonConst.EMPTY_OBJECT_ARRAY;
          }
          result = invocableEngine.invokeFunction(functionName, args);

        } else {
          // INVOKE THE CODE SNIPPET
          final Object[] args = iArgs == null ? null : iArgs.values().toArray();
          result = scriptEngine.eval(scriptManager.getFunctionInvoke(f, args), binding);
        }
        return OCommandExecutorUtility.transformResult(
            scriptManager.handleResult(f.getLanguage(), result, scriptEngine, binding, db));

      } catch (ScriptException e) {
        throw OException.wrapException(
            new OCommandScriptException(
                "Error on execution of the script", functionName, e.getColumnNumber()),
            e);
      } catch (NoSuchMethodException e) {
        throw OException.wrapException(
            new OCommandScriptException("Error on execution of the script", functionName, 0), e);
      } catch (OCommandScriptException e) {
        // PASS THROUGH
        throw e;

      } finally {
        scriptManager.unbind(scriptEngine, binding, context, iArgs);
      }
    } finally {
      scriptManager.releaseDatabaseEngine(f.getLanguage(), db.getName(), scriptEngine);
    }
  }
}
