package com.orientechnologies.tinkerpop.handler;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OGremlinPluginLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "gremlin";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OGraphServerHandler();
  }
}
