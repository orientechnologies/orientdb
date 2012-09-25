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
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;

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

  public Object executeInContext(final Map<String, Object> iContext, final Map<Object, Object> iArgs) {

    parserText = request.getText();

    final ODatabaseRecordTx db = (ODatabaseRecordTx) getDatabase();

    final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(parserText);
    final OScriptManager scriptManager = Orient.instance().getScriptManager();
    final ScriptEngine scriptEngine = scriptManager.getEngine(f.getLanguage());
    final Bindings binding = scriptManager.bind(scriptEngine, db, iContext, iArgs);

    try {
      scriptEngine.setBindings(binding, ScriptContext.ENGINE_SCOPE);

      // COMPILE FUNCTION LIBRARY
      scriptEngine.eval(scriptManager.getLibrary(db, f.getLanguage()));

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
        return scriptEngine.eval(invokeFunction(f, iArgs.values().toArray()), binding);
      }
    } catch (ScriptException e) {
      throw new OCommandScriptException("Error on execution of the script", request.getText(), e.getColumnNumber(), e);
    } catch (NoSuchMethodException e) {
      throw new OCommandScriptException("Error on execution of the script", request.getText(), 0, e);
      
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

  protected String invokeFunction(final OFunction f, Object[] iArgs) {
    final StringBuilder code = new StringBuilder();

    code.append(f.getName());
    code.append('(');
    int i = 0;
    for (Object a : iArgs) {
      if (i++ > 0)
        code.append(',');
      code.append(a);
    }
    code.append(");");

    return code.toString();
  }

}
