package com.orientechnologies.orient.core.command.script.polyglot;

import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.List;

public class OPolyglotScriptExecutorRegister implements OScriptExecutorRegister {

  @Override
  public void registerExecutor(OrientDBEmbedded ctx, OScriptManager scriptManager) {
    for (String lang : getLanguages()) {
      scriptManager.registerScriptExecutor(
          lang,
          new OPolyglotScriptExecutor(ctx, scriptManager, lang, new OPolyglotTransformerImpl()));
    }
  }

  @Override
  public List<String> getLanguages() {
    return List.of("javascript", "ecmascript");
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
