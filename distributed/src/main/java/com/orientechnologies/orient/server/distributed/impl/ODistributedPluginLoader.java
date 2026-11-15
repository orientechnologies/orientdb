package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class ODistributedPluginLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "distributed";
  }

  @Override
  public OServerPlugin newInstance() {
    return new ODistributedPlugin();
  }

  @Override
  public String getPluginClassName() {
    return ODistributedPlugin.class.getName();
  }

  @Override
  public Class getConfigClass() {
    return null;
  }

  @Override
  public OServerPlugin newInstance(OServer context, Object config) {
    return new ODistributedPlugin(context, config);
  }
}
