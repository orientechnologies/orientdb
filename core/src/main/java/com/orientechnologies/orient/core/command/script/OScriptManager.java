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

import com.orientechnologies.common.concur.resource.OPartitionedObjectPool;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.formatter.*;
import com.orientechnologies.orient.core.command.script.js.OJSScriptEngineFactory;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformerImpl;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OConfigurationException;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.metadata.function.OFunctionUtilWrapper;
import com.orientechnologies.orient.core.sql.OSQLScriptEngine;
import com.orientechnologies.orient.core.sql.OSQLScriptEngineFactory;

import javax.script.*;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;

import static com.orientechnologies.common.util.OClassLoaderHelper.lookupProviderWithOrientClassLoader;

/**
 * Executes Script Commands.
 *
 * @author Luca Garulli (l.garulli--(at)--orientdb.com)
 * @see OCommandScript
 */
public class OScriptManager {
  protected static final Object[]                                          EMPTY_PARAMS       = new Object[] {};
  protected static final int                                               LINES_AROUND_ERROR = 5;
  protected final        String                                            DEF_LANGUAGE       = "javascript";
  protected              String                                            defaultLanguage    = DEF_LANGUAGE;
  protected              ScriptEngineManager                               scriptEngineManager;
  protected              Map<String, ScriptEngineFactory>                  engines            = new HashMap<String, ScriptEngineFactory>();
  protected              Map<String, OScriptFormatter>                     formatters         = new HashMap<String, OScriptFormatter>();
  protected              List<OScriptInjection>                            injections         = new ArrayList<OScriptInjection>();
  protected              ConcurrentHashMap<String, ODatabaseScriptManager> dbManagers         = new ConcurrentHashMap<String, ODatabaseScriptManager>();
  protected              Map<String, OScriptResultHandler>                 handlers           = new HashMap<String, OScriptResultHandler>();
  protected              Map<String, Function<String, OScriptExecutor>>    executorsFactories = new HashMap<>();

  public OScriptManager() {
    scriptEngineManager = new ScriptEngineManager();

    executorsFactories.put("javascript", (lang) -> new OJavascriptScriptExecutor(lang, new OScriptTransformerImpl()));
    executorsFactories.put("ecmascript", (lang) -> new OJavascriptScriptExecutor(lang, new OScriptTransformerImpl()));

    for (ScriptEngineFactory f : scriptEngineManager.getEngineFactories()) {
      registerEngine(f.getLanguageName().toLowerCase(Locale.ENGLISH), f);

      if (defaultLanguage == null)
        defaultLanguage = f.getLanguageName();
    }

    if (!existsEngine(DEF_LANGUAGE)) {
      final ScriptEngine defEngine = scriptEngineManager.getEngineByName(DEF_LANGUAGE);
      if (defEngine == null) {
        OLogManager.instance().warnNoDb(this, "Cannot find default script language for %s", DEF_LANGUAGE);
      } else {
        // GET DIRECTLY THE LANGUAGE BY NAME (DON'T KNOW WHY SOMETIMES DOESN'T RETURN IT WITH getEngineFactories() ABOVE!
        registerEngine(DEF_LANGUAGE, defEngine.getFactory());
        defaultLanguage = DEF_LANGUAGE;
      }
    }

    registerFormatter(OSQLScriptEngine.NAME, new OSQLScriptFormatter());
    registerFormatter(DEF_LANGUAGE, new OJSScriptFormatter());
    registerFormatter("ruby", new ORubyScriptFormatter());
    registerFormatter("groovy", new OGroovyScriptFormatter());
    for (String lang : engines.keySet()) {
      Function<String, OScriptExecutor> factory = executorsFactories.get(lang);
      OScriptExecutor executor = null;
      if (factory != null) {
        executor = factory.apply(lang);
      } else {
        executor = new OJsr223ScriptExecutor(lang, new OScriptTransformerImpl());
      }
      OCommandManager.instance().registerScriptExecutor(lang, executor);
    }

    // Registring sql script engine after for not fight with the basic engine
    registerEngine(OSQLScriptEngine.NAME, new OSQLScriptEngineFactory());

    Iterator<OScriptExecutorRegister> customExecutors = lookupProviderWithOrientClassLoader(OScriptExecutorRegister.class);

    customExecutors.forEachRemaining(e -> e.registerExecutor(this, OCommandManager.instance()));

  }

  public String getFunctionDefinition(final OFunction iFunction) {
    final OScriptFormatter formatter = formatters.get(iFunction.getLanguage().toLowerCase(Locale.ENGLISH));
    if (formatter == null)
      throw new IllegalArgumentException("Cannot find script formatter for the language '" + iFunction.getLanguage() + "'");

    return formatter.getFunctionDefinition(iFunction);
  }

  public String getFunctionInvoke(final OFunction iFunction, final Object[] iArgs) {
    final OScriptFormatter formatter = formatters.get(iFunction.getLanguage().toLowerCase(Locale.ENGLISH));
    if (formatter == null)
      throw new IllegalArgumentException("Cannot find script formatter for the language '" + iFunction.getLanguage() + "'");

    return formatter.getFunctionInvoke(iFunction, iArgs);
  }

  /**
   * Formats the library of functions for a language.
   *
   * @param db        Current database instance
   * @param iLanguage Language as filter
   *
   * @return String containing all the functions
   */
  public String getLibrary(final ODatabase<?> db, final String iLanguage) {
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

  public boolean existsEngine(String iLanguage) {
    if (iLanguage == null)
      return false;

    iLanguage = iLanguage.toLowerCase(Locale.ENGLISH);
    return engines.containsKey(iLanguage);
  }

  public ScriptEngine getEngine(final String iLanguage) {
    if (iLanguage == null)
      throw new OCommandScriptException("No language was specified");

    final String lang = iLanguage.toLowerCase(Locale.ENGLISH);

    final ScriptEngineFactory scriptEngineFactory = engines.get(lang);
    if (scriptEngineFactory == null)
      throw new OCommandScriptException(
          "Unsupported language: " + iLanguage + ". Supported languages are: " + getSupportedLanguages());

    return scriptEngineFactory.getScriptEngine();
  }

  /**
   * Acquires a database engine from the pool. Once finished using it, the instance MUST be returned in the pool by calling the
   * method #releaseDatabaseEngine(String, ScriptEngine).
   *
   * @param databaseName Database name
   * @param language     Script language
   *
   * @return ScriptEngine instance with the function library already parsed
   *
   * @see #releaseDatabaseEngine(String, String, OPartitionedObjectPool.PoolEntry)
   */
  public OPartitionedObjectPool.PoolEntry<ScriptEngine> acquireDatabaseEngine(final String databaseName, final String language) {
    ODatabaseScriptManager dbManager = dbManagers.get(databaseName);
    if (dbManager == null) {
      // CREATE A NEW DATABASE SCRIPT MANAGER
      dbManager = new ODatabaseScriptManager(this, databaseName);
      final ODatabaseScriptManager prev = dbManagers.putIfAbsent(databaseName, dbManager);
      if (prev != null) {
        dbManager.close();
        // GET PREVIOUS ONE
        dbManager = prev;
      }
    }

    return dbManager.acquireEngine(language);
  }

  /**
   * Acquires a database engine from the pool. Once finished using it, the instance MUST be returned in the pool by calling the
   * method
   *
   * @param iLanguage     Script language
   * @param iDatabaseName Database name
   * @param poolEntry     Pool entry to free
   *
   * @see #acquireDatabaseEngine(String, String)
   */
  public void releaseDatabaseEngine(final String iLanguage, final String iDatabaseName,
      final OPartitionedObjectPool.PoolEntry<ScriptEngine> poolEntry) {
    final ODatabaseScriptManager dbManager = dbManagers.get(iDatabaseName);
    // We check if there is still a valid pool because it could be removed by the function reload
    if (dbManager != null) {
      dbManager.releaseEngine(iLanguage, poolEntry);
    }

  }

  public Iterable<String> getSupportedLanguages() {
    final HashSet<String> result = new HashSet<String>();
    result.addAll(engines.keySet());
    return result;
  }

  public Bindings bindContextVariables(ScriptEngine engine, final Bindings binding, final ODatabaseDocumentInternal db,
      final OCommandContext iContext, final Map<Object, Object> iArgs) {

    binding.put("db", new OScriptDatabaseWrapper(db));

    bindInjectors(engine, binding, db);

    bindContext(binding, iContext);

    bindParameters(binding, iArgs);

    return binding;
  }

  @Deprecated
  public Bindings bind(ScriptEngine scriptEngine, final Bindings binding, final ODatabaseDocumentInternal db,
      final OCommandContext iContext, final Map<Object, Object> iArgs) {

    bindLegacyDatabaseAndUtil(binding, db);

    bindDatabase(binding, db);

    bindInjectors(scriptEngine, binding, db);

    bindContext(binding, iContext);

    bindParameters(binding, iArgs);

    return binding;
  }

  private void bindInjectors(ScriptEngine engine, Bindings binding, ODatabaseDocument database) {
    for (OScriptInjection i : injections)
      i.bind(engine, binding, database);
  }

  private void bindContext(Bindings binding, OCommandContext iContext) {
    // BIND CONTEXT VARIABLE INTO THE SCRIPT
    if (iContext != null) {
      binding.put("ctx", iContext);
      for (Entry<String, Object> a : iContext.getVariables().entrySet()) {
        binding.put(a.getKey(), a.getValue());
      }
    }
  }

  private void bindLegacyDatabaseAndUtil(Bindings binding, ODatabaseDocumentInternal db) {
    if (db != null) {
      // BIND FIXED VARIABLES
      //      binding.put("db", new OScriptDocumentDatabaseWrapper(db));
      binding.put("orient", new OScriptOrientWrapper(db));
    }
    binding.put("util", new OFunctionUtilWrapper());
  }

  private void bindDatabase(Bindings binding, ODatabaseDocumentInternal db) {
    if (db != null) {
      binding.put("db", new OScriptDatabaseWrapper(db));
    }
  }

  private void bindParameters(Bindings binding, Map<Object, Object> iArgs) {
    // BIND PARAMETERS INTO THE SCRIPT
    if (iArgs != null) {
      for (Entry<Object, Object> a : iArgs.entrySet()) {
        binding.put(a.getKey().toString(), a.getValue());
      }

      binding.put("params", iArgs.values().toArray());
    } else
      binding.put("params", EMPTY_PARAMS);
  }

  public String throwErrorMessage(final ScriptException e, final String lib) {
    int errorLineNumber = e.getLineNumber();

    if (errorLineNumber <= 0) {
      // FIX TO RHINO: SOMETIMES HAS THE LINE NUMBER INSIDE THE TEXT :-(
      final String excMessage = e.toString();
      final int pos = excMessage.indexOf("<Unknown Source>#");
      if (pos > -1) {
        final int end = excMessage.indexOf(')', pos + "<Unknown Source>#".length());
        String lineNumberAsString = excMessage.substring(pos + "<Unknown Source>#".length(), end);
        errorLineNumber = Integer.parseInt(lineNumberAsString);
      }
    }

    if (errorLineNumber <= 0) {
      throw new OCommandScriptException(
          "Error on evaluation of the script library. Error: " + e.getMessage() + "\nScript library was:\n" + lib);
    } else {
      final StringBuilder code = new StringBuilder();
      final Scanner scanner = new Scanner(lib);
      try {
        scanner.useDelimiter("\n");
        String currentLine = null;
        String lastFunctionName = "unknown";

        for (int currentLineNumber = 1; scanner.hasNext(); currentLineNumber++) {
          currentLine = scanner.next();
          int pos = currentLine.indexOf("function");
          if (pos > -1) {
            final String[] words = OStringParser
                .getWords(currentLine.substring(Math.min(pos + "function".length() + 1, currentLine.length())), " \r\n\t");
            if (words.length > 0 && words[0] != "(")
              lastFunctionName = words[0];
          }

          if (currentLineNumber == errorLineNumber)
            // APPEND X LINES BEFORE
            code.append(String.format("%4d: >>> %s\n", currentLineNumber, currentLine));
          else if (Math.abs(currentLineNumber - errorLineNumber) <= LINES_AROUND_ERROR)
            // AROUND: APPEND IT
            code.append(String.format("%4d: %s\n", currentLineNumber, currentLine));
        }

        code.insert(0, String.format("ScriptManager: error %s.\nFunction %s:\n\n", e.getMessage(), lastFunctionName));

      } finally {
        scanner.close();
      }

      throw new OCommandScriptException(code.toString());
    }
  }

  /**
   * Unbinds variables
   *
   * @param binding
   */
  public void unbind(ScriptEngine scriptEngine, final Bindings binding, final OCommandContext iContext,
      final Map<Object, Object> iArgs) {
    for (OScriptInjection i : injections)
      i.unbind(scriptEngine, binding);

    binding.put("db", null);
    binding.put("orient", null);

    binding.put("util", null);

    binding.put("ctx", null);
    if (iContext != null) {
      for (Entry<String, Object> a : iContext.getVariables().entrySet())
        binding.put(a.getKey(), null);
    }

    if (iArgs != null) {
      for (Entry<Object, Object> a : iArgs.entrySet())
        binding.put(a.getKey().toString(), null);

    }
    binding.put("params", null);
  }

  public void registerInjection(final OScriptInjection iInj) {
    if (!injections.contains(iInj))
      injections.add(iInj);
  }

  public void addAllowedPackages(Set<String> packages) {

    this.engines.entrySet().forEach(e -> {
      if (e.getValue() instanceof OSecuredScriptFactory) {
        ((OSecuredScriptFactory) e.getValue()).addAllowedPackages(packages);
      }
    });
  }

  public void removeAllowedPackages(Set<String> packages) {
    this.engines.entrySet().forEach(e -> {
      if (e.getValue() instanceof OSecuredScriptFactory) {
        ((OSecuredScriptFactory) e.getValue()).removeAllowedPackages(packages);
      }
    });
  }

  public void unregisterInjection(final OScriptInjection iInj) {
    injections.remove(iInj);
  }

  public List<OScriptInjection> getInjections() {
    return injections;
  }

  public OScriptManager registerEngine(final String iLanguage, final ScriptEngineFactory iEngine) {
    engines.put(iLanguage, OJSScriptEngineFactory.maybeWrap(iEngine));
    return this;
  }

  public OScriptManager registerFormatter(final String iLanguage, final OScriptFormatter iFormatterImpl) {
    formatters.put(iLanguage.toLowerCase(Locale.ENGLISH), iFormatterImpl);
    return this;
  }

  public OScriptManager registerResultHandler(final String iLanguage, final OScriptResultHandler resultHandler) {
    handlers.put(iLanguage.toLowerCase(Locale.ENGLISH), resultHandler);
    return this;
  }

  public Object handleResult(String language, Object result, ScriptEngine engine, Bindings binding, ODatabaseDocument database) {
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
   * Closes the pool for a database. This is called at Orient shutdown and in case a function has been updated.
   *
   * @param iDatabaseName
   */
  public void close(final String iDatabaseName) {
    final ODatabaseScriptManager dbPool = dbManagers.remove(iDatabaseName);
    if (dbPool != null)
      dbPool.close();
  }

}
