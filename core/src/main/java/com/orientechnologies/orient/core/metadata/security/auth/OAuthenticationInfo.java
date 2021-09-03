package com.orientechnologies.orient.core.metadata.security.auth;

import java.util.Optional;

public interface OAuthenticationInfo {

  public Optional<String> getDatabase();
}
