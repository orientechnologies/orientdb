package com.orientechnologies.orient.graph.handler;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.config.OServerParameterConfiguration;
import com.orientechnologies.orient.server.handler.OServerHandlerAbstract;

public class OGraphServerHandler extends OServerHandlerAbstract {
  private boolean enabled      = true;
  private int     graphPoolMax = OGlobalConfiguration.DB_POOL_MAX.getValueAsInteger();

  @Override
  public void config(OServer oServer, OServerParameterConfiguration[] iParams) {
    for (OServerParameterConfiguration param : iParams) {
      if (param.name.equalsIgnoreCase("enabled")) {
        if (!Boolean.parseBoolean(param.value))
          // DISABLE IT
          return;
      } else if (param.name.equalsIgnoreCase("graph.pool.max"))
        graphPoolMax = Integer.parseInt(param.value);
    }

    enabled = true;
    OLogManager.instance().info(this, "Installing GREMLIN language v.%s - graph.pool.max=%d", OGremlinHelper.getEngineVersion(),
        graphPoolMax);
  }

  @Override
  public String getName() {
    return "graph";
  }

  @Override
  public void startup() {
    if (!enabled)
      return;

    OGremlinHelper.global().setMaxGraphPool(graphPoolMax).create();
  }

  @Override
  public void shutdown() {
    if (!enabled)
      return;

    OGremlinHelper.global().destroy();
  }
}
