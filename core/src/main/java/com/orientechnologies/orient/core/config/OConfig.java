package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfig {
  private String databases;
  private String backup;
  // Maybe ?
  // private String pluginsPath;
  private ODefaultDatabaseConfig defaultDatabaseConfig;
  private OConfigSecurity security;
  private List<OConfigNetwork> listeners;
  private OConfigPlugins plugins;
  private OConfigDistributed distributed;
  private List<OHookConfig> hooks;
  // This is mostly for legacy support,
  // on the long term this should be replaced by something else.
  private List<OConfigProperty> propreties;

  public void setDatabases(String databases) {
    this.databases = databases;
  }

  public String getDatabases() {
    return databases;
  }

  public void setBackup(String backup) {
    this.backup = backup;
  }

  public String getBackup() {
    return backup;
  }

  public void setPlugins(OConfigPlugins plugins) {
    this.plugins = plugins;
  }

  public List<OConfigNetwork> getListeners() {
    return listeners;
  }

  public void setListeners(List<OConfigNetwork> listeners) {
    this.listeners = listeners;
  }
}
