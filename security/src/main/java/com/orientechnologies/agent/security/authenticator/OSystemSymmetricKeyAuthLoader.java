package com.orientechnologies.agent.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class OSystemSymmetricKeyAuthLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "system-symmetric-key";
  }

  @Override
  public String getClassName() {
    return OSystemSymmetricKeyAuth.class.getName();
  }

  @Override
  public OSecurityAuthenticator newInstance() {
    return new OSystemSymmetricKeyAuth();
  }
}
