package com.orientechnologies.orient.core.command.script.jsr223;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.command.script.OCommandExecutorUtility;
import com.orientechnologies.orient.core.command.script.OCommandScriptException;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.js.OJSScriptEngineFactory;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptException;

/** Created by tglman on 25/01/17. */
public class OJsr223ScriptExecutor extends OAbstractScriptExecutor {
  protected static final int LINES_AROUND_ERROR = 5;
  private final OScriptTransformer transformer;
  private final ConcurrentMap<String, OResourcePool<ODatabaseSession, ScriptEngine>> pooledEngines =
      new ConcurrentHashMap<>();
  private final OrientDBEmbedded context;
  private final ScriptEngineFactory factory;
  private final OScriptManager scriptManager;

  public OJsr223ScriptExecutor(
      String language,
      OrientDBEmbedded context,
      OScriptTransformer scriptTransformer,
      OScriptManager scriptManager,
      ScriptEngineFactory factory) {
    super(language);
    this.transformer = scriptTransformer;
    this.context = context;
    this.scriptManager = scriptManager;
    this.factory = OJSScriptEngineFactory.maybeWrap(factory);
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

    final ScriptEngine scriptEngine = getEngine(database);
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
      releaseEngine(database.getName(), scriptEngine);
    }
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) context.getDatabase();
    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(functionName);

    db.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, f.getName());

    final OScriptManager scriptManager = db.getSharedContext().getOrientDB().getScriptManager();

    final ScriptEngine scriptEngine = getEngine(db);
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
      releaseEngine(db.getName(), scriptEngine);
    }
  }

  private ScriptEngine createEngine(ODatabaseSession db) {
    final ScriptEngine scriptEngine = factory.getScriptEngine();
    final String library =
        scriptManager.getLibrary(ODatabaseRecordThreadLocal.instance().get(), language);

    if (library != null)
      try {
        scriptEngine.eval(library);
      } catch (ScriptException e) {
        OAbstractScriptExecutor.throwErrorMessage(e, library);
      }
    return scriptEngine;
  }

  private ScriptEngine getEngine(ODatabaseSession database) {

    OResourcePool<ODatabaseSession, ScriptEngine> p =
        pooledEngines.computeIfAbsent(
            database.getName(),
            (key) -> {
              int poolSize =
                  database.getConfiguration().getValueAsInteger(OGlobalConfiguration.SCRIPT_POOL);
              return new OResourcePool<ODatabaseSession, ScriptEngine>(
                  poolSize,
                  new OResourcePoolListener<ODatabaseSession, ScriptEngine>() {

                    public ScriptEngine createNewResource(ODatabaseSession iKey) {
                      return createEngine(iKey);
                    }

                    public boolean reuseResource(ODatabaseSession iKey, ScriptEngine iValue) {
                      return true;
                    }
                  });
            });

    return p.getResource(database, 0L);
  }

  private void releaseEngine(String dbName, ScriptEngine engine) {
    pooledEngines.get(dbName).returnResource(engine);
  }

  @Override
  public void close(String iDatabaseName) {
    OResourcePool<ODatabaseSession, ScriptEngine> pool = pooledEngines.remove(iDatabaseName);
    if (pool != null) {
      pool.close();
    }
  }

  @Override
  public void closeAll() {
    Set<String> dbNames = new HashSet<>(pooledEngines.keySet());
    for (String db : dbNames) {
      close(db);
    }
  }
}
