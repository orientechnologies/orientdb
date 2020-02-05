package com.orientechnologies.orient.core.command.script.js;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineFactory;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class OLazyScriptEngineFactory implements ScriptEngineFactory {

  private ScriptEngineFactory engineFactory;

  private static final List<String> WRAPPED_LANGUAGES = Arrays.asList("javascript", "ecmascript");

  public OLazyScriptEngineFactory(ScriptEngineFactory engineFactory) {
    this.engineFactory = engineFactory;
  }

  @Override
  public String getEngineName() {
    return engineFactory.getEngineName();
  }

  @Override
  public String getEngineVersion() {
    return engineFactory.getEngineVersion();
  }

  @Override
  public List<String> getExtensions() {
    return engineFactory.getExtensions();
  }

  @Override
  public List<String> getMimeTypes() {
    return engineFactory.getMimeTypes();
  }

  @Override
  public List<String> getNames() {
    return engineFactory.getNames();
  }

  @Override
  public String getLanguageName() {
    return engineFactory.getLanguageName();
  }

  @Override
  public String getLanguageVersion() {
    return engineFactory.getLanguageVersion();
  }

  @Override
  public Object getParameter(String key) {
    return engineFactory.getParameter(key);
  }

  @Override
  public String getMethodCallSyntax(String obj, String m, String... args) {
    return engineFactory.getMethodCallSyntax(obj, m, args);
  }

  @Override
  public String getOutputStatement(String toDisplay) {
    return engineFactory.getOutputStatement(toDisplay);
  }

  @Override
  public String getProgram(String... statements) {
    return engineFactory.getProgram(statements);
  }

  @Override
  public synchronized ScriptEngine getScriptEngine() {

    if (!engineFactory.getClass().getName()
        .equalsIgnoreCase("com.orientechnologies.orient.core.command.script.js.ONashornScriptEngineFactory") && engineFactory
        .getClass().getName().equalsIgnoreCase("jdk.nashorn.api.scripting.NashornScriptEngineFactory")) {
      engineFactory = new ONashornScriptEngineFactory(engineFactory);
    }
    return engineFactory.getScriptEngine();
  }

  public static ScriptEngineFactory maybeWrap(ScriptEngineFactory engineFactory) {

    if (WRAPPED_LANGUAGES.contains(engineFactory.getLanguageName().toLowerCase(Locale.ENGLISH))) {
      return new OLazyScriptEngineFactory(engineFactory);
    }
    return engineFactory;
  }
}
