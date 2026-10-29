package com.orientechnologies.orient.core.security;

public class ODefaultCILoader implements OCredentialInterceptorLoader {

  @Override
  public String getName() {
    return "default";
  }

  @Override
  public String getClassName() {
    return ODefaultCI.class.getName();
  }

  @Override
  public OCredentialInterceptor newInstance() {
    return new ODefaultCI();
  }
}
