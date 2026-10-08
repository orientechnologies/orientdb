package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OCustomSQLFunctionPluginLoader implements OServerPluginLoader {

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
}
