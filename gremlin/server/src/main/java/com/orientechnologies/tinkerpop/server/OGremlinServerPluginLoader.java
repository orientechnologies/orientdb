package com.orientechnologies.tinkerpop.server;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OGremlinServerPluginLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "gremlin-server";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OGremlinServerPlugin();
  }

  @Override
  public String getPluginClassName() {
    return OGremlinServerPlugin.class.getName();
  }

  @Override
  public Class getConfigClass() {
    return null;
  }

  @Override
  public OServerPlugin newInstance(OServer context, Object config) {
    return new OGremlinServerPlugin(context, config);
  }
}
