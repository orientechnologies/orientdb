package com.orientechnologies.agent.security.authenticator;

import com.orientechnologies.orient.core.security.OSecurityAuthenticator;
import com.orientechnologies.orient.core.security.OSecurityAuthenticatorLoader;

public class OSecuritySymmetricKeyAuthLoader implements OSecurityAuthenticatorLoader {

  @Override
  public String getName() {
    return "symmetric-key";
  }

  @Override
  public String getClassName() {
    return OSecuritySymmetricKeyAuth.class.getName();
  }

  @Override
  public OSecurityAuthenticator newInstance() {
    return new OSecuritySymmetricKeyAuth();
  }
}
