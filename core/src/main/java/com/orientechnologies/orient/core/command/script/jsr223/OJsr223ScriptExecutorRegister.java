package com.orientechnologies.orient.core.command.script.jsr223;

import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformerImpl;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

public class OJsr223ScriptExecutorRegister implements OScriptExecutorRegister {

  private final ScriptEngineManager scriptEngineManager;
  private final Map<String, ScriptEngineFactory> languages;

  public OJsr223ScriptExecutorRegister() {
    scriptEngineManager = new ScriptEngineManager();

    languages = new HashMap<>();
    for (ScriptEngineFactory f : scriptEngineManager.getEngineFactories()) {
      languages.put(f.getLanguageName(), f);
    }
  }

  @Override
  public void registerExecutor(OrientDBEmbedded ctx, OScriptManager scriptManager) {
    for (var lang : languages.entrySet()) {
      scriptManager.registerScriptExecutor(
          lang.getKey(),
          new OJsr223ScriptExecutor(
              lang.getKey(), ctx, new OScriptTransformerImpl(), scriptManager, lang.getValue()));
    }
  }

  @Override
  public List<String> getLanguages() {
    return new ArrayList<>(languages.keySet());
  }

  @Override
  public int getPriority() {
    return 0;
  }
}
