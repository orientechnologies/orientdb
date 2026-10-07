package com.orientechnologies.tinkerpop.server;

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
}
