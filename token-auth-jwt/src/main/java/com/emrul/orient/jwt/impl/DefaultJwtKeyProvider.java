package com.emrul.orient.jwt.impl;

import com.orientechnologies.orient.core.metadata.security.jwt.IJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.IJwtKeyProvider;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class DefaultJwtKeyProvider implements IJwtKeyProvider {

  private SecretKeySpec secret_key;

  public DefaultJwtKeyProvider(byte[] secret) {
    secret_key = new SecretKeySpec(secret, "HmacSHA256");
  }

  @Override
  public Key getKey(IJwtHeader header) {
    return secret_key;
  }
}
