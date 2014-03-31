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

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.Map;

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

    if (language.equalsIgnoreCase("SQL"))
      // SPECIAL CASE: EXECUTE THE COMMANDS IN SEQUENCE
      return executeSQL();
    else
      return executeJsr223Script(language, iContext, iArgs);
  }

  public boolean isIdempotent() {
    return false;
  }

  protected Object executeJsr223Script(final String language, final OCommandContext iContext, final Map<Object, Object> iArgs) {
    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !(db instanceof ODatabaseRecordTx))
      db = db.getUnderlying();

    final OScriptManager scriptManager = Orient.instance().getScriptManager();
    CompiledScript compiledScript = request.getCompiledScript();

    if (compiledScript == null) {
      ScriptEngine scriptEngine = scriptManager.getEngine(language);
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

  // TODO: CREATE A REGULAR JSR223 SCRIPT IMPL
  protected Object executeSQL() {
    ODatabaseRecord db = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (db != null && !(db instanceof ODatabaseRecordTx))
      db = db.getUnderlying();

    final BufferedReader reader = new BufferedReader(new StringReader(parserText));
    String lastCommand = "";
    Object lastResult = null;
    try {
      while ((lastCommand = reader.readLine()) != null) {
        lastCommand = lastCommand.trim();

        if (OStringSerializerHelper.startsWithIgnoreCase(lastCommand, "let ")) {
          final int equalsPos = lastCommand.indexOf('=');
          final String variable = lastCommand.substring("let ".length(), equalsPos).trim();
          final String cmd = lastCommand.substring(equalsPos + 1).trim();

          lastResult = db.command(new OCommandSQL(cmd).setContext(getContext())).execute();

          // PUT THE RESULT INTO THE CONTEXT
          getContext().setVariable(variable, lastResult);
        } else if (lastCommand.equalsIgnoreCase("begin"))
          db.begin();
        else if (lastCommand.equalsIgnoreCase("commit"))
          db.commit();
        else if (lastCommand.equalsIgnoreCase("rollback"))
          db.rollback();
        else if (OStringSerializerHelper.startsWithIgnoreCase(lastCommand, "return ")) {
          final String variable = lastCommand.substring("return ".length()).trim();

          if (variable.equalsIgnoreCase("NULL"))
            lastResult = null;
          else if (variable.startsWith("$"))
            lastResult = getContext().getVariable(variable);
          else
            lastResult = variable;

          // END OF THE SCRIPT
          break;

        } else
          lastResult = db.command(new OCommandSQL(lastCommand).setContext(getContext())).execute();

      }
    } catch (IOException e) {
      throw new OCommandExecutionException("Error on executing command: " + lastCommand, e);
    }
    return lastResult;
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandScriptException("Error on execution of the script: " + iText, request.getText(), 0);
  }
}
