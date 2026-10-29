package com.orientechnologies.orient.core.security.kerberos;

import com.orientechnologies.orient.core.security.OCredentialInterceptor;
import com.orientechnologies.orient.core.security.OCredentialInterceptorLoader;

public class OKerberosCredentialInterceptorLoader implements OCredentialInterceptorLoader {

  @Override
  public String getName() {
    return "kerberos";
  }

  @Override
  public String getClassName() {
    return OKerberosCredentialInterceptor.class.getName();
  }

  @Override
  public OCredentialInterceptor newInstance() {
    return new OKerberosCredentialInterceptor();
  }
}
