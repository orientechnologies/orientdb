package com.orientechnologies.orient.etl;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OETLPluginLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "etl";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OETLPlugin();
  }

  @Override
  public String getPluginClassName() {
    return OETLPlugin.class.getName();
  }
}
