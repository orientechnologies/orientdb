package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OAutomaticBackupLoader implements OServerPluginLoader {

  @Override
  public String getName() {
    return "automaticBackup";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OAutomaticBackup();
  }

  @Override
  public String getPluginClassName() {
    return OAutomaticBackup.class.getName();
  }
}
