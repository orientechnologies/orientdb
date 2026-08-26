package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfig {
  private String databases;
  private String backup;
  private ODefaultDatabaseConfig defaultDatabaseConfig;
  private OConfigSecurity security;
  private List<OConfigNetwork> network;
  private List<OConfigPlugin> plugin;
  private OConfigDistributed distributed;

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
}
