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

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandExecutorAbstract;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.OCommandSQLParsingException;

import javax.script.Bindings;
import javax.script.Compilable;
import javax.script.CompiledScript;
import javax.script.ScriptEngine;
import javax.script.ScriptException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

      if (!(scriptEngine instanceof Compilable))
        throw new OCommandExecutionException("Language '" + language + "' does not support compilation");

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
      final Object ob = compiledScript.eval(binding);

      return OCommandExecutorUtility.transformResult(ob);
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

    try {

      return executeSQLScript(db, parserText);

    } catch (IOException e) {
      throw new OCommandExecutionException("Error on executing command: " + parserText, e);
    }
  }

  @Override
  protected void throwSyntaxErrorException(String iText) {
    throw new OCommandScriptException("Error on execution of the script: " + iText, request.getText(), 0);
  }

  protected Object executeSQLScript(ODatabaseRecord db, final String iText) throws IOException {
    Object lastResult = null;
    int txBegunAtLine = -1;
    int txBegunAtPart = -1;
    int maxRetry = 1;

    context.setVariable("transactionRetries", 0);

    for (int retry = 0; retry < maxRetry; retry++) {

      final BufferedReader reader = new BufferedReader(new StringReader(iText));

      int line = 0;
      int linePart = 0;
      String lastLine;
      boolean txBegun = false;

      for (; line < txBegunAtLine; ++line)
        // SKIP PREVIOUS COMMAND AND JUMP TO THE BEGIN IF ANY
        lastLine = reader.readLine();

      for (; (lastLine = reader.readLine()) != null; ++line) {
        lastLine = lastLine.trim();

        final List<String> lineParts = OStringSerializerHelper.smartSplit(lastLine, ';');

        if (line == txBegunAtLine)
          // SKIP PREVIOUS COMMAND PART AND JUMP TO THE BEGIN IF ANY
          linePart = txBegunAtPart;
        else
          linePart = 0;

        for (; linePart < lineParts.size(); ++linePart) {
          final String lastCommand = lineParts.get(linePart);

          if (OStringSerializerHelper.startsWithIgnoreCase(lastCommand, "let ")) {
            final int equalsPos = lastCommand.indexOf('=');
            final String variable = lastCommand.substring("let ".length(), equalsPos).trim();
            final String cmd = lastCommand.substring(equalsPos + 1).trim();

            lastResult = db.command(new OCommandSQL(cmd).setContext(getContext())).execute();

            // PUT THE RESULT INTO THE CONTEXT
            getContext().setVariable(variable, lastResult);
          } else if (lastCommand.equalsIgnoreCase("begin")) {

            if (txBegun)
              throw new OCommandSQLParsingException("Transaction already begun");

            txBegun = true;
            txBegunAtLine = line;
            txBegunAtPart = linePart;

            db.begin();

          } else if (lastCommand.equalsIgnoreCase("rollback")) {

            if (!txBegun)
              throw new OCommandSQLParsingException("Transaction not begun");

            db.rollback();

            txBegun = false;
            txBegunAtLine = -1;
            txBegunAtPart = -1;

          } else if (OStringSerializerHelper.startsWithIgnoreCase(lastCommand, "commit")) {
            if (txBegunAtLine < 0)
              throw new OCommandSQLParsingException("Transaction not begun");

            if (lastCommand.length() > "commit ".length()) {
              String next = lastCommand.substring("commit ".length()).trim();
              if (OStringSerializerHelper.startsWithIgnoreCase(next, "retry ")) {
                next = next.substring("retry ".length()).trim();
                maxRetry = Integer.parseInt(next) + 1;
              }
            }

            try {
              db.commit();
            } catch (OConcurrentModificationException e) {
              context.setVariable("transactionRetries", retry);
              break;
            }

            txBegun = false;
            txBegunAtLine = -1;
            txBegunAtPart = -1;

          } else if (OStringSerializerHelper.startsWithIgnoreCase(lastCommand, "sleep ")) {

            final String sleepTimeInMs = lastCommand.substring("sleep ".length()).trim();
            try {
              Thread.sleep(Integer.parseInt(sleepTimeInMs));
            } catch (InterruptedException e) {
            }

          } else if (OStringSerializerHelper.startsWithIgnoreCase(lastCommand, "return ")) {

            final String variable = lastCommand.substring("return ".length()).trim();

            if (variable.equalsIgnoreCase("NULL"))
              lastResult = null;
            else if (variable.startsWith("$"))
              lastResult = getContext().getVariable(variable);
            else if (variable.startsWith("[") && variable.endsWith("]")) {
              // ARRAY - COLLECTION
              final List<String> items = new ArrayList<String>();

              OStringSerializerHelper.getCollection(variable, 0, items);
              final List<Object> result = new ArrayList<Object>(items.size());

              for (int i = 0; i < items.size(); ++i) {
                String item = items.get(i);

                Object res;
                if (item.startsWith("$"))
                  res = getContext().getVariable(item);
                else
                  res = item;

                if (OMultiValue.isMultiValue(res) && OMultiValue.getSize(res) == 1)
                  res = OMultiValue.getFirstValue(res);

                result.add(res);
              }
              lastResult = result;
            } else if (variable.startsWith("{") && variable.endsWith("}")) {
              // MAP
              final Map<String, String> map = OStringSerializerHelper.getMap(variable);
              final Map<Object, Object> result = new HashMap<Object, Object>(map.size());

              for (Map.Entry<String, String> entry : map.entrySet()) {
                // KEY
                String stringKey = entry.getKey();
                if (stringKey == null)
                  continue;

                stringKey = stringKey.trim();

                Object key;
                if (stringKey.startsWith("$"))
                  key = getContext().getVariable(stringKey);
                else
                  key = stringKey;

                if (OMultiValue.isMultiValue(key) && OMultiValue.getSize(key) == 1)
                  key = OMultiValue.getFirstValue(key);

                // VALUE
                String stringValue = entry.getValue();
                if (stringValue == null)
                  continue;

                stringValue = stringValue.trim();

                Object value;
                if (stringValue.toString().startsWith("$"))
                  value = getContext().getVariable(stringValue);
                else
                  value = stringValue;

                if (OMultiValue.isMultiValue(value) && OMultiValue.getSize(value) == 1)
                  value = OMultiValue.getFirstValue(value);

                result.put(key, value);
              }
              lastResult = result;
            } else
              lastResult = variable;

            // END OF THE SCRIPT
            return lastResult;

          } else if (lastCommand != null && lastCommand.length() > 0)
            lastResult = db.command(new OCommandSQL(lastCommand).setContext(getContext())).execute();
        }
      }
    }

    return lastResult;
  }
}
