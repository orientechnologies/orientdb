package com.orientechnologies.security;

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
}
