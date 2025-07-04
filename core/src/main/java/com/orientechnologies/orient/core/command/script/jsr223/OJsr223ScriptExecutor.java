package com.orientechnologies.orient.core.command.script.jsr223;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.command.script.OCommandExecutorUtility;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.ODatabaseScriptPool;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
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
  protected static final int LINES_AROUND_ERROR = 5;
  private final OScriptTransformer transformer;
  private final ODatabaseScriptPool pool;
  private OrientDBEmbedded context;

  public OJsr223ScriptExecutor(
      String language,
      OrientDBEmbedded context,
      OScriptTransformer scriptTransformer,
      ODatabaseScriptPool pool) {
    super(language);
    this.transformer = scriptTransformer;
    this.pool = pool;
    this.context = context;
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

    OScriptManager scriptManager = context.getScriptManager();
    preExecute(database, script, params);

    CompiledScript compiledScript = null;

    final ScriptEngine scriptEngine = pool.acquireDatabaseEngine(database.getName(), language);
    try {

      if (!(scriptEngine instanceof Compilable))
        throw new OCommandExecutionException(
            "Language '" + language + "' does not support compilation");

      final Compilable c = (Compilable) scriptEngine;
      try {
        compiledScript = c.compile(script);
      } catch (ScriptException e) {
        throwErrorMessage(e, script);
      }

      final Bindings binding =
          bindContextVariables(
              compiledScript.getEngine().getBindings(ScriptContext.ENGINE_SCOPE),
              database,
              null,
              params,
              scriptManager);

      try {
        final Object ob = compiledScript.eval(binding);
        return transformer.toResultSet(ob);
      } catch (ScriptException e) {
        throw OException.wrapException(
            new OCommandScriptException(
                "Error on execution of the script", script, e.getColumnNumber()),
            e);

      } finally {
        unbind(binding, null, params, scriptManager);
      }
    } finally {
      pool.releaseDatabaseEngine(language, database.getName(), scriptEngine);
    }
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(functionName);

    db.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, f.getName());

    final OScriptManager scriptManager = db.getSharedContext().getOrientDB().getScriptManager();

    final ScriptEngine scriptEngine = pool.acquireDatabaseEngine(db.getName(), f.getLanguage());
    try {
      final Bindings binding =
          bind(
              scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE),
              db,
              context,
              iArgs,
              scriptManager);

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
        unbind(binding, context, iArgs, scriptManager);
      }
    } finally {
      pool.releaseDatabaseEngine(f.getLanguage(), db.getName(), scriptEngine);
    }
  }
}
