package com.orientechnologies.orient.core.metadata.security.auth;

import com.orientechnologies.orient.core.metadata.security.OToken;
import java.util.Optional;

public class OTokenAuthInfo implements OAuthenticationInfo {

  private OToken token;

  @Override
  public Optional<String> getDatabase() {
    return Optional.ofNullable(token.getDatabase());
  }

  public OToken getToken() {
    return token;
  }
}
