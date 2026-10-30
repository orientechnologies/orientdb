package com.orientechnologies.orient.server.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class OSystemUserAuthenticatorLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "system-users";
  }

  @Override
  public String getClassName() {
    return OSystemUserAuthenticator.class.getName();
  }

  @Override
  public OSecurityAuthenticator newInstance() {
    return new OSystemUserAuthenticator();
  }
}
