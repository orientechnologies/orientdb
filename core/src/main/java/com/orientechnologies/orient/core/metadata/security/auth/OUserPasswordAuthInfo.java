package com.orientechnologies.orient.core.metadata.security.auth;

public class OUserPasswordAuthInfo implements OAuthenticationInfo {

  private String database;
  private String user;
  private String password;

  @Override
  public String getDatabase() {
    return database;
  }

  public String getUser() {
    return user;
  }

  public String getPassword() {
    return password;
  }
}
