package com.orientechnologies.orient.core.command.script.js;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import javax.script.ScriptEngineFactory;

public class OJSScriptEngineFactory {

  private static final List<String> WRAPPED_LANGUAGES = Arrays.asList("javascript", "ecmascript");

  public static ScriptEngineFactory maybeWrap(ScriptEngineFactory engineFactory) {
    final String factoryClassName = engineFactory.getClass().getName();

    if (WRAPPED_LANGUAGES.contains(engineFactory.getLanguageName().toLowerCase(Locale.ENGLISH))
        && (factoryClassName.equalsIgnoreCase(
                "jdk.nashorn.api.scripting.NashornScriptEngineFactory")
            || factoryClassName.equalsIgnoreCase(
                "com.oracle.truffle.js.scriptengine.GraalJSEngineFactory"))) {
      return new OSecuredScriptEngineFactory(engineFactory);
    }
    return engineFactory;
  }
}
