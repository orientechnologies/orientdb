package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.command.OScriptExecutor;
import com.orientechnologies.orient.core.command.OScriptInterceptor;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.function.OFunctionUtilWrapper;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import javax.script.Bindings;
import javax.script.ScriptException;

public abstract class OAbstractScriptExecutor implements OScriptExecutor {
  protected static final Object[] EMPTY_PARAMS = new Object[] {};
  protected static final int LINES_AROUND_ERROR = 5;

  protected final String language;

  public OAbstractScriptExecutor(String language) {
    this.language = language;
  }

  private List<OScriptInterceptor> interceptors = new ArrayList<>();

  @Override
  public void registerInterceptor(OScriptInterceptor interceptor) {
    interceptors.add(interceptor);
  }

  public void preExecute(ODatabaseDocumentInternal database, String script, Object params) {

    interceptors.forEach(i -> i.preExecute(database, language, script, params));
  }

  @Override
  public void unregisterInterceptor(OScriptInterceptor interceptor) {
    interceptors.remove(interceptor);
  }

  public Bindings bindContextVariables(
      final Bindings binding,
      final ODatabaseDocumentInternal db,
      final OCommandContext iContext,
      final Map<Object, Object> iArgs,
      OScriptManager scriptManager) {

    bindDatabase(binding, db);

    bindInjectors(binding, db, scriptManager);

    bindContext(binding, iContext);

    bindParameters(binding, iArgs);

    return binding;
  }

  public Bindings bind(
      final Bindings binding,
      final ODatabaseDocumentInternal db,
      final OCommandContext iContext,
      final Map<Object, Object> iArgs,
      OScriptManager scriptManager) {

    bindLegacyDatabaseAndUtil(binding, db);

    bindDatabase(binding, db);

    bindInjectors(binding, db, scriptManager);

    bindContext(binding, iContext);

    bindParameters(binding, iArgs);

    return binding;
  }

  private void bindInjectors(
      Bindings binding, ODatabaseDocument database, OScriptManager scriptManager) {
    for (OScriptInjection i : scriptManager.getInjections()) i.bind(binding, database);
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
    } else binding.put("params", EMPTY_PARAMS);
  }

  /**
   * Unbinds variables
   * @param binding
   * @param scriptManager TODO
   */
  public void unbind(
      final Bindings binding,
      final OCommandContext iContext,
      final Map<Object, Object> iArgs,
      OScriptManager scriptManager) {
    for (OScriptInjection i : scriptManager.getInjections()) i.unbind(binding);

    binding.put("db", null);
    binding.put("orient", null);

    binding.put("util", null);

    binding.put("ctx", null);
    if (iContext != null) {
      for (Entry<String, Object> a : iContext.getVariables().entrySet())
        binding.put(a.getKey(), null);
    }

    if (iArgs != null) {
      for (Entry<Object, Object> a : iArgs.entrySet()) binding.put(a.getKey().toString(), null);
    }
    binding.put("params", null);
  }

  public static String throwErrorMessage(final ScriptException e, final String lib) {
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
          "Error on evaluation of the script library. Error: "
              + e.getMessage()
              + "\nScript library was:\n"
              + lib);
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
            final String[] words =
                OStringParser.getWords(
                    currentLine.substring(
                        Math.min(pos + "function".length() + 1, currentLine.length())),
                    " \r\n\t");
            if (words.length > 0 && words[0] != "(") lastFunctionName = words[0];
          }

          if (currentLineNumber == errorLineNumber)
            // APPEND X LINES BEFORE
            code.append(String.format("%4d: >>> %s\n", currentLineNumber, currentLine));
          else if (Math.abs(currentLineNumber - errorLineNumber) <= LINES_AROUND_ERROR)
            // AROUND: APPEND IT
            code.append(String.format("%4d: %s\n", currentLineNumber, currentLine));
        }

        code.insert(
            0,
            String.format(
                "ScriptManager: error %s.\nFunction %s:\n\n", e.getMessage(), lastFunctionName));

      } finally {
        scanner.close();
      }

      throw new OCommandScriptException(code.toString());
    }
  }
}
