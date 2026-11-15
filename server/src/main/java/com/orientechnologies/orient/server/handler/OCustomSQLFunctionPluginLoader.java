package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OCustomSQLFunctionPluginLoader
    implements OServerPluginLoader<OCustomSQLFunctionPluginConfig> {

  @Override
  public String getName() {
    return "functions";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OCustomSQLFunctionPlugin();
  }

  @Override
  public String getPluginClassName() {
    return OCustomSQLFunctionPlugin.class.getName();
  }

  @Override
  public Class<OCustomSQLFunctionPluginConfig> getConfigClass() {
    return OCustomSQLFunctionPluginConfig.class;
  }

  @Override
  public OServerPlugin newInstance(OServer context, OCustomSQLFunctionPluginConfig config) {
    return new OCustomSQLFunctionPlugin(context, config);
  }
}
