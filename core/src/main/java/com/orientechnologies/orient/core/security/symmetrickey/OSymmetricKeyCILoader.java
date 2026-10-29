package com.orientechnologies.orient.core.security.symmetrickey;

import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OCredentialInterceptorLoader;

public class OSymmetricKeyCILoader implements OCredentialInterceptorLoader {

  @Override
  public String getName() {
    return "symmetric-key";
  }

  @Override
  public String getClassName() {
    return OSymmetricKeyCI.class.getName();
  }

  @Override
  public OCredentialInterceptor newInstance() {
    return new OSymmetricKeyCI();
  }
}
