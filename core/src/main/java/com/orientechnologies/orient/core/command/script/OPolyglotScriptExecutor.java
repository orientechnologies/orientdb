package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ORole;
import com.orientechnologies.orient.core.metadata.security.ORule;
import com.orientechnologies.orient.core.sql.executor.OResultInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import javax.script.ScriptException;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

/** Created by Luca Garulli */
public class OPolyglotScriptExecutor extends OAbstractScriptExecutor
    implements OResourcePoolListener<ODatabaseDocumentInternal, Context> {
  private final OScriptTransformer transformer;
  protected ConcurrentHashMap<String, OResourcePool<ODatabaseDocumentInternal, Context>>
      contextPools =
          new ConcurrentHashMap<String, OResourcePool<ODatabaseDocumentInternal, Context>>();

  public OPolyglotScriptExecutor(final String language, OScriptTransformer scriptTransformer) {
    super("javascript".equalsIgnoreCase(language) ? "js" : language);
    this.transformer = scriptTransformer;
  }

  private Context resolveContext(ODatabaseDocumentInternal database) {
    OResourcePool<ODatabaseDocumentInternal, Context> pool =
        contextPools.computeIfAbsent(
            database.getName(),
            (k) -> {
              return new OResourcePool<ODatabaseDocumentInternal, Context>(
                  database.getConfiguration().getValueAsInteger(OGlobalConfiguration.SCRIPT_POOL),
                  OPolyglotScriptExecutor.this);
            });
    return pool.getResource(database, 0);
  }

  private void returnContext(Context context, ODatabaseDocumentInternal database) {
    OResourcePool<ODatabaseDocumentInternal, Context> pool = contextPools.get(database.getName());
    if (pool != null) {
      pool.returnResource(context);
    }
  }

  @Override
  public Context createNewResource(ODatabaseDocumentInternal database, Object... iAdditionalArgs) {
    final OScriptManager scriptManager =
        database.getSharedContext().getOrientDB().getScriptManager();

    final Set<String> allowedPackaged = scriptManager.getAllowedPackages();

    Context ctx =
        Context.newBuilder()
            .allowHostAccess(HostAccess.ALL)
            .allowNativeAccess(false)
            .allowCreateProcess(false)
            .allowCreateThread(false)
            .allowIO(false)
            .allowHostClassLoading(false)
            .allowHostClassLookup(
                s -> {
                  if (allowedPackaged.contains(s)) return true;

                  final int pos = s.lastIndexOf('.');
                  if (pos > -1) return allowedPackaged.contains(s.substring(0, pos) + ".*");
                  return false;
                })
            .build();

    OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

    scriptManager.bindContextVariables(null, bindings, database, null, null);
    final String library =
        scriptManager.getLibrary(
            database, "js".equalsIgnoreCase(language) ? "javascript" : language);
    if (library != null) {
      ctx.eval(language, library);
    }
    scriptManager.unbind(null, bindings, null, null);
    return ctx;
  }

  @Override
  public boolean reuseResource(
      ODatabaseDocumentInternal iKey, Object[] iAdditionalArgs, Context iValue) {
    return true;
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

    Context ctx = resolveContext(database);
    try {
      OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, params);

      Value result = ctx.eval(language, script);
      OResultSet transformedResult = transformer.toResultSet(result);
      scriptManager.unbind(null, bindings, null, null);
      return transformedResult;

    } catch (PolyglotException e) {
      final int col = e.getSourceLocation() != null ? e.getSourceLocation().getStartColumn() : 0;
      throw OException.wrapException(
          new OCommandScriptException("Error on execution of the script", script, col),
          new ScriptException(e));
    } finally {
      returnContext(ctx, database);
    }
  }

  @Override
  public Object executeFunction(
      OCommandContext context, final String functionName, final Map<Object, Object> iArgs) {

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) context.getDatabase();
    if (database == null) {
      database = ODatabaseRecordThreadLocal.instance().get();
    }
    final OFunction f = database.getMetadata().getFunctionLibrary().getFunction(functionName);

    database.checkSecurity(ORule.ResourceGeneric.FUNCTION, ORole.PERMISSION_READ, f.getName());

    final OScriptManager scriptManager =
        database.getSharedContext().getOrientDB().getScriptManager();

    Context ctx = resolveContext(database);
    try {

      OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, iArgs);
      final Object[] args = iArgs == null ? null : iArgs.keySet().toArray();

      Value result = ctx.eval(language, scriptManager.getFunctionInvoke(f, args));

      Object finalResult;
      if (result.isNull()) {
        finalResult = null;
      } else if (result.hasArrayElements()) {
        final List<Object> array = new ArrayList<>((int) result.getArraySize());
        for (int i = 0; i < result.getArraySize(); ++i)
          array.add(new OResultInternal(result.getArrayElement(i).asHostObject()));
        finalResult = array;
      } else if (result.isHostObject()) {
        finalResult = result.asHostObject();
      } else if (result.isString()) {
        finalResult = result.asString();
      } else if (result.isNumber()) {
        finalResult = result.asDouble();
      } else {
        finalResult = result;
      }
      scriptManager.unbind(null, bindings, null, null);
      return finalResult;
    } catch (PolyglotException e) {
      final int col = e.getSourceLocation() != null ? e.getSourceLocation().getStartColumn() : 0;
      throw OException.wrapException(
          new OCommandScriptException("Error on execution of the script", functionName, col),
          new ScriptException(e));
    } finally {
      returnContext(ctx, database);
    }
  }

  @Override
  public void close(String iDatabaseName) {
    OResourcePool<ODatabaseDocumentInternal, Context> contextPool =
        contextPools.remove(iDatabaseName);
    if (contextPool != null) {
      for (Context c : contextPool.getAllResources()) {
        c.close();
      }
      contextPool.close();
    }
  }

  @Override
  public void closeAll() {
    for (OResourcePool<ODatabaseDocumentInternal, Context> d : contextPools.values()) {
      for (Context c : d.getAllResources()) {
        c.close();
      }
      d.close();
    }
    contextPools.clear();
  }
}
