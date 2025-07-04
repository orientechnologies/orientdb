package com.orientechnologies.orient.core.command.script.jsr223;

import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.ODatabaseScriptPool;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.command.script.transformer.OScriptTransformerImpl;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.ArrayList;
import java.util.List;
import javax.script.ScriptEngineFactory;
import javax.script.ScriptEngineManager;

public class OJsr223ScriptExecutorRegister implements OScriptExecutorRegister {

  private final ScriptEngineManager scriptEngineManager;
  private final List<String> languages;

  public OJsr223ScriptExecutorRegister() {
    scriptEngineManager = new ScriptEngineManager();

    languages = new ArrayList<>();
    for (ScriptEngineFactory f : scriptEngineManager.getEngineFactories()) {
      languages.add(f.getLanguageName());
    }
  }

  @Override
  public void registerExecutor(
      OrientDBEmbedded ctx, OScriptManager scriptManager, OCommandManager commandManager) {
    ODatabaseScriptPool pool = scriptManager.getDatabaseScriptPool();
    for (String lang : getLanguages()) {
      commandManager.registerScriptExecutor(
          lang, new OJsr223ScriptExecutor(lang, ctx, new OScriptTransformerImpl(), pool));
    }
  }

  @Override
  public List<String> getLanguages() {
    return languages;
  }

  @Override
  public int getPriority() {
    return 0;
  }
}
