package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OJMXPluginLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "jmx";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OJMXPlugin();
  }
}
