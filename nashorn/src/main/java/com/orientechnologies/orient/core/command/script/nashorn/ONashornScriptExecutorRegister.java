package com.orientechnologies.orient.core.command.script.nashorn;

import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.List;
import javax.script.ScriptEngineManager;
import org.openjdk.nashorn.api.scripting.NashornScriptEngineFactory;

public class ONashornScriptExecutorRegister implements OScriptExecutorRegister {

  private final ScriptEngineManager scriptEngineManager;

  public ONashornScriptExecutorRegister() {
    scriptEngineManager = new ScriptEngineManager();
  }

  @Override
  public void registerExecutor(OrientDBEmbedded ctx, OScriptManager scriptManager) {
    for (var lang : getLanguages()) {
      NashornScriptEngineFactory factory =
          (NashornScriptEngineFactory) scriptEngineManager.getEngineByName("nashorn").getFactory();
      var executor = new ONashornScriptExecutor(lang, ctx, scriptManager, factory);
      scriptManager.registerScriptExecutor(lang, executor);
    }
  }

  @Override
  public List<String> getLanguages() {
    return List.of("javascript", "ecmascript");
  }

  @Override
  public int getPriority() {
    return 0;
  }
}
