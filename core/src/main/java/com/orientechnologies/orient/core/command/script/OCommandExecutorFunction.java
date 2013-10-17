/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.core.command.script;

import java.util.Map;
import java.util.Map.Entry;

import javax.script.Bindings;
import javax.script.Invocable;
import javax.script.ScriptContext;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.security.ODatabaseSecurityResources;
import com.orientechnologies.orient.core.metadata.security.ORole;

/**
 * Executes Script Commands.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorFunction extends OCommandExecutorAbstract {
  protected OCommandFunction request;

  public OCommandExecutorFunction() {
  }

  @SuppressWarnings("unchecked")
  public OCommandExecutorFunction parse(final OCommandRequest iRequest) {
    request = (OCommandFunction) iRequest;
    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    return executeInContext(null, iArgs);
  }

  public Object executeInContext(final OCommandContext iContext, final Map<Object, Object> iArgs) {

    parserText = request.getText();

    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !(db instanceof ODatabaseRecordTx))
      db = db.getUnderlying();

    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(parserText);

    db.checkSecurity(ODatabaseSecurityResources.FUNCTION, ORole.PERMISSION_READ, f.getName());

    final OScriptManager scriptManager = Orient.instance().getScriptManager();
    final ScriptEngine scriptEngine = scriptManager.getEngine(f.getLanguage());
    final Bindings binding = scriptManager.bind(scriptEngine.getBindings(ScriptContext.ENGINE_SCOPE), (ODatabaseRecordTx) db,
        iContext, iArgs);

    try {
      // COMPILE FUNCTION LIBRARY
      final String lib = scriptManager.getLibrary(db, f.getLanguage());
      if (lib != null)
        try {
          scriptEngine.eval(lib);
        } catch (ScriptException e) {
          scriptManager.getErrorMessage(e, lib);
        }

      if (scriptEngine instanceof Invocable) {
        // INVOKE AS FUNCTION. PARAMS ARE PASSED BY POSITION
        final Invocable invocableEngine = (Invocable) scriptEngine;
        Object[] args = null;
        if (iArgs != null) {
          args = new Object[iArgs.size()];
          int i = 0;
          for (Entry<Object, Object> arg : iArgs.entrySet())
            args[i++] = arg.getValue();
        }
        return invocableEngine.invokeFunction(parserText, args);

      } else {
        // INVOKE THE CODE SNIPPET
        final Object[] args = iArgs == null ? null : iArgs.values().toArray();
        return scriptEngine.eval(scriptManager.getFunctionInvoke(f, args), binding);
      }
    } catch (ScriptException e) {
      throw new OCommandScriptException("Error on execution of the script", request.getText(), e.getColumnNumber(), e);
    } catch (NoSuchMethodException e) {
      throw new OCommandScriptException("Error on execution of the script", request.getText(), 0, e);
    } catch (OCommandScriptException e) {
      // PASS THROUGH
      throw e;

    } finally {
      scriptManager.unbind(binding);
    }
  }

  public boolean isIdempotent() {
    return false;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandScriptException("Error on execution of the script: " + iText, request.getText(), 0);
  }
}
