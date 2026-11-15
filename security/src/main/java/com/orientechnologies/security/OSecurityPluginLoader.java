package com.orientechnologies.security;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OSecurityPluginLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "security";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OSecurityPlugin();
  }

  @Override
  public String getPluginClassName() {
    return OSecurityPlugin.class.getName();
  }

  @Override
  public Class getConfigClass() {
    return null;
  }

  @Override
  public OServerPlugin newInstance(OServer context, Object config) {
    return new OSecurityPlugin();
  }
}
