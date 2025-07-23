package com.orientechnologies.orient.core.command.script.polyglot;

import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.Collections;
import java.util.List;

public class OPolyglotScriptExecutorRegister implements OScriptExecutorRegister {

  @Override
  public void registerExecutor(OrientDBEmbedded ctx, OScriptManager scriptManager) {
    final boolean useGraal = OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean();
    if (useGraal) {
      for (String lang : getLanguages()) {
        scriptManager.registerScriptExecutor(
            lang,
            new OPolyglotScriptExecutor(ctx, scriptManager, lang, new OPolyglotTransformerImpl()));
      }
    }
  }

  @Override
  public List<String> getLanguages() {
    final boolean useGraal = OGlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean();
    if (useGraal) {
      return List.of("javascript", "ecmascript");
    } else {
      return Collections.emptyList();
    }
  }

  @Override
  public int getPriority() {
    return 1;
  }
}
