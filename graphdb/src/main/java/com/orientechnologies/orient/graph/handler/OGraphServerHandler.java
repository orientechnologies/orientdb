package com.orientechnologies.orient.graph.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;
import com.tinkerpop.gremlin.jsr223.GremlinScriptEngineFactory;

public class OGraphServerHandler extends OServerHandlerAbstract {
   @Override
   public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
      OLogManager.instance().info(this, "Installing GREMLIN language v.%s", new GremlinScriptEngineFactory().getEngineVersion());
   }

   @Override
   public String getName() {
      return "graph";
   }

   @Override
   public void startup() {
      OGremlinHelper.global().create();
   }

   @Override
   public void shutdown() {
      OGremlinHelper.global().destroy();
   }
}
