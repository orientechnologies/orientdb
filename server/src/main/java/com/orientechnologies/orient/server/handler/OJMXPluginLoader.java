package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OJMXPluginLoader implements OServerPluginLoader<OJMXPluginConfig> {

  @Override
  public String getName() {
    return "jmx";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OJMXPlugin();
  }

  @Override
  public String getPluginClassName() {
    return OJMXPlugin.class.getName();
  }

  @Override
  public Class<OJMXPluginConfig> getConfigClass() {
    return OJMXPluginConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, OJMXPluginConfig config) {
    return new OJMXPlugin(context, config);
  }
}
