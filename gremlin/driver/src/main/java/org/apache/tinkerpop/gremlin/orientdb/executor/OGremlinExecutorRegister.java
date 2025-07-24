package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.db.OrientDBEmbedded;
import java.util.List;

/** Created by Enrico Risa on 30/01/17. */
public class OGremlinExecutorRegister implements OScriptExecutorRegister {

  @Override
  public void registerExecutor(OrientDBEmbedded ctx, OScriptManager scriptManager) {
    scriptManager.registerScriptExecutor(
        OCommandGremlinExecutor.GREMLIN,
        new OCommandGremlinExecutor(OCommandGremlinExecutor.GREMLIN, scriptManager));
    scriptManager.registerScriptExecutor(
        OCommandGremlinExecutor.GREMLIN_GROOVY,
        new OCommandGremlinExecutor(OCommandGremlinExecutor.GREMLIN_GROOVY, scriptManager));
  }

  @Override
  public List<String> getLanguages() {
    return List.of("gremlin", "gremlin-groovy");
  }

  @Override
  public int getPriority() {
    return 2;
  }
}
