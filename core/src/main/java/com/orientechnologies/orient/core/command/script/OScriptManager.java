/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.command.script;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.OSqlScriptExecutor;
import com.orientechnologies.orient.core.command.script.formatter.OGroovyScriptFormatter;
import com.orientechnologies.orient.core.command.script.formatter.OJSScriptFormatter;
import com.orientechnologies.orient.core.command.script.formatter.ORubyScriptFormatter;
import com.orientechnologies.orient.core.command.script.formatter.OSQLScriptFormatter;
import com.orientechnologies.orient.core.command.script.formatter.OScriptFormatter;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import javax.script.Bindings;
import javax.script.ScriptEngine;

/**
 * Executes Script Commands.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 */
public class OScriptManager {
  protected static final String DEF_LANGUAGE = "javascript";
  protected Map<String, OScriptFormatter> formatters = new HashMap<>();
  protected List<OScriptInjection> injections = new ArrayList<>();
  protected Map<String, OScriptResultHandler> handlers = new HashMap<>();
  private Set<String> allowedPackages = Collections.newSetFromMap(new ConcurrentHashMap<>());
  private Map<String, OScriptExecutor> scriptExecutors = new HashMap<>();

  public OScriptManager(OrientDBEmbedded context) {
    registerFormatter("sql", new OSQLScriptFormatter());
    registerFormatter(DEF_LANGUAGE, new OJSScriptFormatter());
    registerFormatter("ruby", new ORubyScriptFormatter());
    registerFormatter("groovy", new OGroovyScriptFormatter());

    registerScriptExecutor("sql", new OSqlScriptExecutor());
    registerScriptExecutor("script", new OSqlScriptExecutor());

    Iterator<OScriptExecutorRegister> customExecutors =
        lookupProviderWithOrientClassLoader(OScriptExecutorRegister.class);
    Set<OScriptExecutorRegister> registers =
        new TreeSet<>((x, y) -> x.getPriority() - y.getPriority());
    while (customExecutors.hasNext()) {
      OScriptExecutorRegister x = customExecutors.next();
      registers.add(x);
    }
    registers.forEach(e -> e.registerExecutor(context, this));
  }

  public String getFunctionDefinition(final OFunction iFunction) {
    final OScriptFormatter formatter =
        formatters.get(iFunction.getLanguage().toLowerCase(Locale.ENGLISH));
    if (formatter == null)
      throw new IllegalArgumentException(
          "Cannot find script formatter for the language '" + iFunction.getLanguage() + "'");

    return formatter.getFunctionDefinition(iFunction);
  }

  public String getFunctionInvoke(final OFunction iFunction, final Object[] iArgs) {
    final OScriptFormatter formatter =
        formatters.get(iFunction.getLanguage().toLowerCase(Locale.ENGLISH));
    if (formatter == null)
      throw new IllegalArgumentException(
          "Cannot find script formatter for the language '" + iFunction.getLanguage() + "'");

    return formatter.getFunctionInvoke(iFunction, iArgs);
  }

  /**
   * Formats the library of functions for a language.
   *
   * @param db Current database instance
   * @param iLanguage Language as filter
   * @return String containing all the functions
   */
  public String getLibrary(final ODatabaseDocumentInternal db, final String iLanguage) {
    if (db == null)
      // NO DB = NO LIBRARY
      return null;

    final StringBuilder code = new StringBuilder();

    final Set<String> functions = db.getMetadata().getFunctionLibrary().getFunctionNames();
    for (String fName : functions) {
      final OFunction f = db.getMetadata().getFunctionLibrary().getFunction(fName);

      if (f.getLanguage() == null)
        throw new OConfigurationException("Database function '" + fName + "' has no language");

      if (f.getLanguage().equalsIgnoreCase(iLanguage)) {
        final String def = getFunctionDefinition(f);
        if (def != null) {
          code.append(def);
          code.append("\n");
        }
      }
    }

    return code.length() == 0 ? null : code.toString();
  }

  public Iterable<String> getSupportedLanguages() {
    return Collections.unmodifiableSet(this.scriptExecutors.keySet());
  }

  public void registerInjection(final OScriptInjection iInj) {
    if (!injections.contains(iInj)) injections.add(iInj);
  }

  public Set<String> getAllowedPackages() {
    return Collections.unmodifiableSet(allowedPackages);
  }

  public void addAllowedPackages(Set<String> packages) {
    allowedPackages.addAll(packages);
    closeAll();
  }

  public void removeAllowedPackages(Set<String> packages) {
    allowedPackages.removeAll(packages);
    closeAll();
  }

  public void unregisterInjection(final OScriptInjection iInj) {
    injections.remove(iInj);
  }

  public List<OScriptInjection> getInjections() {
    return injections;
  }

  public OScriptManager registerFormatter(
      final String iLanguage, final OScriptFormatter iFormatterImpl) {
    formatters.put(iLanguage.toLowerCase(Locale.ENGLISH), iFormatterImpl);
    return this;
  }

  public OScriptManager registerResultHandler(
      final String iLanguage, final OScriptResultHandler resultHandler) {
    handlers.put(iLanguage.toLowerCase(Locale.ENGLISH), resultHandler);
    return this;
  }

  public Object handleResult(
      String language,
      Object result,
      ScriptEngine engine,
      Bindings binding,
      ODatabaseDocument database) {
    OScriptResultHandler handler = handlers.get(language);
    if (handler != null) {
      return handler.handle(result, engine, binding, database);
    } else {
      return result;
    }
  }

  /**
   * Ask to the Script engine all the formatters
   *
   * @return Map containing all the formatters
   */
  public Map<String, OScriptFormatter> getFormatters() {
    return formatters;
  }

  /**
   * Closes the pool for a database. This is called at Orient shutdown and in case a function has
   * been updated.
   *
   * @param iDatabaseName
   */
  public void close(final String iDatabaseName) {
    for (OScriptExecutor executor : scriptExecutors.values()) {
      executor.close(iDatabaseName);
    }
  }

  public void closeAll() {
    for (OScriptExecutor executor : scriptExecutors.values()) {
      executor.closeAll();
    }
  }

  public OScriptExecutor getScriptExecutor(String language) {
    if (language == null) {
      throw new IllegalArgumentException("Invalid script languange: null");
    }
    OScriptExecutor scriptExecutor = this.scriptExecutors.get(language);
    if (scriptExecutor == null) {
      scriptExecutor = this.scriptExecutors.get(language.toLowerCase(Locale.ENGLISH));
    }
    if (scriptExecutor == null)
      throw new IllegalArgumentException(
          "Cannot find a script executor requester for language: " + language);

    return scriptExecutor;
  }

  public void registerScriptExecutor(String language, OScriptExecutor executor) {
    this.scriptExecutors.put(language, executor);
  }

  public Map<String, OScriptExecutor> getScriptExecutors() {
    return scriptExecutors;
  }
}
