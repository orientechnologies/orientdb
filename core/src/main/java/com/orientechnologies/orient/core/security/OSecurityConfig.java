package com.orientechnologies.orient.core.security;

public interface OSecurityConfig {

  boolean existsUser(String user);

  OGlobalUser getUser(String username);

  OSyslog getSyslog();

  String getConfigurationFile();
}
