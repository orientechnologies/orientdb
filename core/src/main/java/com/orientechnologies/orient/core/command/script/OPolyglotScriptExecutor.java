package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformer;
import com.orientechnologies.orient.core.command.traverse.OAbstractScriptExecutor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;

import javax.script.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

/**
 * Created by Luca Garulli
 */
public class OPolyglotScriptExecutor extends OAbstractScriptExecutor {
    private final OScriptTransformer transformer;

    public OPolyglotScriptExecutor(final String language, OScriptTransformer scriptTransformer) {
        super(language);
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

        try {
            ScriptEngine engine = new ScriptEngineManager().getEngineByName("JavaScript");

            final Bindings bindings = engine.getBindings(ScriptContext.ENGINE_SCOPE);
            bindings.put("polyglot.js.allowHostAccess", true);
            bindings.put("polyglot.js.allowNativeAccess", false);
            bindings.put("polyglot.js.allowCreateThread", false);
            bindings.put("polyglot.js.allowIO", false);
            bindings.put("polyglot.js.allowHostClassLoading", false);

            final Set<String> allowedPackaged = scriptManager.getAllowedPackages();

            bindings.put("polyglot.js.allowHostClassLookup", (Predicate<String>) s -> {
                if (allowedPackaged.contains(s))
                    return true;

                final int pos = s.lastIndexOf('.');
                if (pos > -1)
                    return allowedPackaged.contains(s.substring(0, pos) + ".*");
                return false;
            });

            scriptManager.bindContextVariables(
                    engine,
                    bindings,
                    database,
                    null,
                    params);

            Object result = engine.eval(script);
            return transformer.toResultSet(result);

        } catch (ScriptException e) {
            throw OException.wrapException(
                    new OCommandScriptException(
                            "Error on execution of the script", script, e.getColumnNumber()),
                    e);
        }
    }

}
