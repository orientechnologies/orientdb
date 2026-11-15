package com.orientechnologies.orient.server.distributed.impl;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class ODistributedPluginLoader implements OServerPluginLoader<ODistributedPluginConfig> {

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
  public Class<ODistributedPluginConfig> getConfigClass() {
    return ODistributedPluginConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, ODistributedPluginConfig config) {
    return new ODistributedPlugin(context, config);
  }
}
