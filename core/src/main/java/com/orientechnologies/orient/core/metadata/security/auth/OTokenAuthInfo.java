package com.orientechnologies.orient.core.metadata.security.auth;

import com.orientechnologies.orient.core.metadata.security.OToken;

public class OTokenAuthInfo implements OAuthenticationInfo {

  private OToken token;

  @Override
  public String getDatabase() {
    return token.getDatabase();
  }

  public OToken getToken() {
    return token;
  }
}
