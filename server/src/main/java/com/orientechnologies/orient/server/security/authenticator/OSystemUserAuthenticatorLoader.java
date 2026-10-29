package com.orientechnologies.orient.server.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class OSystemUserAuthenticatorLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "server-config";
  }

  @Override
  public String getClassName() {
    return OServerConfigAuthenticator.class.getName();
  }

  @Override
  public OSecurityAuthenticator newInstance() {
    return new OServerConfigAuthenticator();
  }
}
