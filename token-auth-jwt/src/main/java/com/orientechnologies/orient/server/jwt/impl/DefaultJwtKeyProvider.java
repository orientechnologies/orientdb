package com.orientechnologies.orient.server.jwt.impl;

import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtKeyProvider;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class DefaultJwtKeyProvider implements OJwtKeyProvider {

  private SecretKeySpec secret_key;

  public DefaultJwtKeyProvider(byte[] secret) {
    secret_key = new SecretKeySpec(secret, "HmacSHA256");
  }

  @Override
  public Key getKey(OJwtHeader header) {
    return secret_key;
  }
}
