package com.orientechnologies.orient.core.security;

public class ODefaultSecurityConfig implements OSecurityConfig {

  @Override
  public boolean existsUser(String user) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setUser(String user, String password, String permissions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void dropUser(String iUserName) {
    throw new UnsupportedOperationException();
  }

  @Override
  public void saveConfiguration() {
    throw new UnsupportedOperationException();
  }

  @Override
  public void setEphemeralUser(String iName, String iPassword, String iPermissions) {
    throw new UnsupportedOperationException();
  }

  @Override
  public OGlobalUser getUser(String username) {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean usersManagement() {
    return false;
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
