package com.orientechnologies.orient.core.config;

import java.util.List;

public class OConfigAuthentication {

  private boolean allowDefault;
  private List<OConfigAuthenticator> authenticators;

  public OConfigAuthentication() {}

  public void setAllowDefault(boolean allowDefault) {
    this.allowDefault = allowDefault;
  }

  public void setAuthenticators(List<OConfigAuthenticator> authenticators) {
    this.authenticators = authenticators;
  }

  public boolean isAllowDefault() {
    return allowDefault;
  }

  public List<OConfigAuthenticator> getAuthenticators() {
    return authenticators;
  }
}
