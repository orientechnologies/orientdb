package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class OServerConfigAuthenticatorLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "server-config";
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
