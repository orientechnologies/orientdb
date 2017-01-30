package org.apache.tinkerpop.gremlin.orientdb.executor;

import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.command.OScriptExecutorRegister;
import com.orientechnologies.orient.core.command.script.OScriptManager;

/**
 * Created by Enrico Risa on 30/01/17.
 */
public class OGremlinExecutorRegister implements OScriptExecutorRegister {

    @Override
    public void registerExecutor(OScriptManager scriptManager, OCommandManager commandManager) {
        commandManager.registerScriptExecutor("gremlin", new OCommandGremlinExecutor(scriptManager));
    }
}
