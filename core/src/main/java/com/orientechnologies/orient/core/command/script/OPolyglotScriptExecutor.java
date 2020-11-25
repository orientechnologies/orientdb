package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.PolyglotException;
import org.graalvm.polyglot.Value;

import javax.script.ScriptException;

/** Created by Luca Garulli */
public class OPolyglotScriptExecutor extends OAbstractScriptExecutor {
  private final OScriptTransformer transformer;

  public OPolyglotScriptExecutor(final String language, OScriptTransformer scriptTransformer) {
    super("javascript".equalsIgnoreCase(language) ? "js" : language);
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

    final Set<String> allowedPackaged = scriptManager.getAllowedPackages();

    try (Context ctx =
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
            .build()) {

      OPolyglotScriptBinding bindings = new OPolyglotScriptBinding(ctx.getBindings(language));

      scriptManager.bindContextVariables(null, bindings, database, null, params);

      Value result = ctx.eval(language, script);

      return transformer.toResultSet(result);

    } catch (PolyglotException e) {
      final int col = e.getSourceLocation() != null ? e.getSourceLocation().getStartColumn() : 0;
      throw OException.wrapException(
          new OCommandScriptException("Error on execution of the script", script, col),
          new ScriptException(e));
    }
  }
}
