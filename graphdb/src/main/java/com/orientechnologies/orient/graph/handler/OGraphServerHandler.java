package com.orientechnologies.orient.graph.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandManager;
import com.orientechnologies.orient.core.sql.OSQLEngine;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlinExecutor;
import com.orientechnologies.orient.graph.gremlin.OSQLFunctionGremlin;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;

public class OGraphServerHandler extends OServerHandlerAbstract {
	@Override
	public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
		OLogManager.instance().info(this, "Installing GREMLIN language v.%s", new GremlinScriptEngineFactory().getEngineVersion());
		OCommandManager.instance().registerRequester("gremlin", OCommandGremlin.class);
		OCommandManager.instance().registerExecutor(OCommandGremlin.class, OCommandGremlinExecutor.class);

		OSQLEngine.getInstance().registerFunction(OSQLFunctionGremlin.NAME, OSQLFunctionGremlin.class);
	}

	@Override
	public String getName() {
		return "graph";
	}
}
