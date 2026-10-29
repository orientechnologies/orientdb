package com.orientechnologies.orient.core.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class ODatabaseUserAuthenticatorLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "database-users";
  }

  @Override
  public String getClassName() {
    return ODatabaseUserAuthenticator.class.getName();
  }

  @Override
  public OSecurityAuthenticator newInstance() {
    return new ODatabaseUserAuthenticator();
  }
}
