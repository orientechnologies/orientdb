package com.orientechnologies.orient.core.security;

public interface OSecurityConfig {

  boolean existsUser(String user);

  void setUser(String user, String password, String permissions);

  void dropUser(String iUserName);

  void saveConfiguration();

  void setEphemeralUser(String iName, String iPassword, String iPermissions);

  OGlobalUser getUser(String username);

  boolean usersManagement();

  OSyslog getSyslog();

  String getConfigurationFile();
}
