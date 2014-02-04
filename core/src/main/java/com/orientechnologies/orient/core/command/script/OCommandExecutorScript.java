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

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;

/**
 * Executes Script Commands.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class OCommandExecutorScript extends OCommandExecutorAbstract {
  protected OCommandScript request;

  public OCommandExecutorScript() {
  }

  @SuppressWarnings("unchecked")
  public OCommandExecutorScript parse(final OCommandRequest iRequest) {
    request = (OCommandScript) iRequest;
    return this;
  }

  public Object execute(final Map<Object, Object> iArgs) {
    return executeInContext(context, iArgs);
  }

  public Object executeInContext(final OCommandContext iContext, final Map<Object, Object> iArgs) {
    final String language = request.getLanguage();
    parserText = request.getText();

    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !(db instanceof ODatabaseRecordTx))
      db = db.getUnderlying();

    final OScriptManager scriptManager = Orient.instance().getScriptManager();
    CompiledScript compiledScript = request.getCompiledScript();

    if (compiledScript == null) {
      ScriptEngine scriptEngine = scriptManager.getEngine(language);
      scriptEngine.put("engine", scriptEngine);
      // COMPILE FUNCTION LIBRARY
      String lib = scriptManager.getLibrary(db, language);
      if (lib == null)
        lib = "";

      parserText = lib + parserText;

      Compilable c = (Compilable) scriptEngine;
      try {
        compiledScript = c.compile(parserText);
      } catch (ScriptException e) {
        scriptManager.getErrorMessage(e, parserText);
      }

      request.setCompiledScript(compiledScript);
    }

    final Bindings binding = scriptManager.bind(compiledScript.getEngine().createBindings(), (ODatabaseRecordTx) db, iContext,
        iArgs);

    try {
      return compiledScript.eval(binding);

    } catch (ScriptException e) {
      throw new OCommandScriptException("Error on execution of the script", request.getText(), e.getColumnNumber(), e);

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
