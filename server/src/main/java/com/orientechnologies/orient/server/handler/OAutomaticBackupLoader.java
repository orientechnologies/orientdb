package com.orientechnologies.orient.server.handler;

import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.plugin.OServerPlugin;
import com.orientechnologies.orient.server.plugin.OServerPluginLoader;

public class OAutomaticBackupLoader implements OServerPluginLoader<OAtomaticBackupConfig> {

  @Override
  public String getName() {
    return "automaticBackup";
  }

  @Override
  public OServerPlugin newInstance() {
    return new OAutomaticBackup();
  }

  @Override
  public OServerPlugin newInstance(OServer context, OAtomaticBackupConfig config) {
    return new OAutomaticBackup(context, config);
  }

  @Override
  public String getPluginClassName() {
    return OAutomaticBackup.class.getName();
  }

  @Override
  public Class<OAtomaticBackupConfig> getConfigClass() {
    return OAtomaticBackupConfig.class;
  }
}
