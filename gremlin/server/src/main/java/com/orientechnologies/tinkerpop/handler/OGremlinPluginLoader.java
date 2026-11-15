package com.orientechnologies.tinkerpop.handler;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OGremlinPluginLoader implements OServerPluginLoader<OGraphServerHandlerConfig> {

  @Override
  public String getName() {
    return "gremlin";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OGraphServerHandler();
  }

  @Override
  public String getPluginClassName() {
    return OGraphServerHandler.class.getName();
  }

  @Override
  public Class<OGraphServerHandlerConfig> getConfigClass() {
    return OGraphServerHandlerConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, OGraphServerHandlerConfig config) {
    return new OGraphServerHandler(context, config);
  }
}
