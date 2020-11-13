package com.orientechnologies.orient.core.security;

public class ODefaultSecurityConfig implements OSecurityConfig {

  @Override
  public boolean existsUser(String user) {
    return false;
  }

  @Override
  public OGlobalUser getUser(String username) {
    return null;
  }

  @Override
  public OSyslog getSyslog() {
    throw new UnsupportedOperationException();
  }

  @Override
  public String getConfigurationFile() {
    return null;
  }
}
