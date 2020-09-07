package com.orientechnologies.orient.server.security;

import com.orientechnologies.orient.server.config.OServerUserConfiguration;

public interface OSecurityConfig {

  boolean existsUser(String user);

  void setUser(String user, String password, String permissions);

  void dropUser(String iUserName);

  void saveConfiguration();

  void setEphemeralUser(String iName, String iPassword, String iPermissions);

  OServerUserConfiguration getUser(String username);

  OSyslog getSyslog();

  String getConfigurationFile();
}
