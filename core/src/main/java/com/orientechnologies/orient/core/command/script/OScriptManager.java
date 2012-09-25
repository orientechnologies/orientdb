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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.script.Bindings;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

import com.orientechnologies.orient.core.command.script.formatter.OJSScriptFormatter;
import com.orientechnologies.orient.core.command.script.formatter.ORubyScriptFormatter;
import com.orientechnologies.orient.core.command.script.formatter.OScriptFormatter;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordTx;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.OSQLScriptEngine;

/**
 * Executes Script Commands.
 * 
 * @see OCommandScript
 * @author Luca Garulli
 * 
 */
public class OScriptManager {
  protected final String                  DEF_LANGUAGE    = "javascript";
  protected ScriptEngineManager           scriptEngineManager;
  protected Map<String, ScriptEngine>     engines;
  protected String                        defaultLanguage = DEF_LANGUAGE;
  protected Map<String, OScriptFormatter> formatters      = new HashMap<String, OScriptFormatter>();
  protected List<OScriptInjection>        injections      = new ArrayList<OScriptInjection>();

  public OScriptManager() {
    if (engines == null) {
      engines = new HashMap<String, ScriptEngine>();
      scriptEngineManager = new ScriptEngineManager();
      for (ScriptEngineFactory f : scriptEngineManager.getEngineFactories()) {
        registerEngine(f.getLanguageName().toLowerCase(), f.getScriptEngine());

        if (defaultLanguage == null)
          defaultLanguage = f.getLanguageName();
      }

      if (!engines.containsKey(DEF_LANGUAGE)) {
        registerEngine(DEF_LANGUAGE, scriptEngineManager.getEngineByName(DEF_LANGUAGE));
        defaultLanguage = DEF_LANGUAGE;
      }

      if (!engines.containsKey(OSQLScriptEngine.ENGINE))
        registerEngine(DEF_LANGUAGE, scriptEngineManager.getEngineByName(DEF_LANGUAGE));

      registerFormatter(DEF_LANGUAGE, new OJSScriptFormatter());
      registerFormatter("ruby", new ORubyScriptFormatter());
    }
  }

  public String getFunction(final OFunction iFunction) {
    final OScriptFormatter formatter = formatters.get(iFunction.getLanguage().toLowerCase());
    if (formatter == null)
      throw new IllegalArgumentException("Cannot find script formatter for the language '" + DEF_LANGUAGE + "'");

    return formatter.getFunction(iFunction);
  }

  /**
   * Format the library of functions for a language.
   * 
   * @param db
   *          Current database instance
   * @param iLanguage
   *          Language as filter
   * @return String containing all the functions
   */
  public String getLibrary(final ODatabaseComplex<?> db, final String iLanguage) {
    final StringBuilder code = new StringBuilder();

    final String[] functions = db.getMetadata().getFunctionLibrary().getFunctionNames();
    for (String fName : functions) {
      final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(fName);

      if (f.getLanguage().equalsIgnoreCase(iLanguage)) {
        code.append(getFunction(f));
        code.append("\n");
      }
    }

    return code.toString();
  }

  public ScriptEngine getEngine(final String iLanguage) {
    if (iLanguage == null)
      throw new OCommandScriptException("No language was specified");

    final String lang = iLanguage.toLowerCase();
    if (!engines.containsKey(lang))
      throw new OCommandScriptException("Unsupported language: " + iLanguage + ". Supported languages are: " + engines);

    final ScriptEngine scriptEngine = engines.get(lang);
    if (scriptEngine == null)
      throw new OCommandScriptException("Cannot find script engine: " + iLanguage);

    return scriptEngine;
  }

  public Bindings bind(final ScriptEngine iEngine, final ODatabaseRecordTx db, final Map<String, Object> iContext,
      final Map<Object, Object> iArgs) {
    final Bindings binding = iEngine.createBindings();

    for (OScriptInjection i : injections)
      i.bind(binding);

    // BIND FIXED VARIABLES
    binding.put("db", new OScriptDocumentDatabaseWrapper(db));
    binding.put("gdb", new OScriptGraphDatabaseWrapper(db));

    // BIND CONTEXT VARIABLE INTO THE SCRIPT
    if (iContext != null) {
      for (Entry<String, Object> a : iContext.entrySet())
        binding.put(a.getKey(), a.getValue());
    }

    // BIND PARAMETERS INTO THE SCRIPT
    if (iArgs != null) {
      for (Entry<Object, Object> a : iArgs.entrySet())
        binding.put(a.getKey().toString(), a.getValue());
      binding.put("params", iArgs);
    } else
      binding.put("params", new HashMap<Object, Object>());

    return binding;
  }

  /**
   * Unbinds variables
   * 
   * @param binding
   */
  public void unbind(Bindings binding) {
    for (OScriptInjection i : injections)
      i.unbind(binding);
  }

  public void registerInjection(final OScriptInjection iInj) {
    if (!injections.contains(iInj))
      injections.add(iInj);
  }

  public void unregisterInjection(final OScriptInjection iInj) {
    injections.remove(iInj);
  }

  public OScriptManager registerEngine(final String iLanguage, final ScriptEngine iEngine) {
    engines.put(iLanguage, iEngine);
    return this;
  }

  public OScriptManager registerFormatter(final String iLanguage, final OScriptFormatter iFormatterImpl) {
    formatters.put(iLanguage, iFormatterImpl);
    return this;
  }
}
