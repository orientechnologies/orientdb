package com.orientechnologies.orient.server.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class ODefaultPasswordAuthenticatorLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "default";
  }

  @Override
  public String getClassName() {
    return ODefaultPasswordAuthenticator.class.getName();
  }

  @Override
  public OSecurityAuthenticator newInstance() {
    return new ODefaultPasswordAuthenticator();
  }
}
