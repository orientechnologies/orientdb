package com.orientechnologies.orient.server.token;

import java.security.Key;

import javax.crypto.spec.SecretKeySpec;

import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OKeyProvider;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class DefaultKeyProvider implements OKeyProvider {

  private SecretKeySpec secret_key;

  public DefaultKeyProvider(byte[] secret) {
    secret_key = new SecretKeySpec(secret, "HmacSHA256");
  }

  @Override
  public Key getKey(OJwtHeader header) {
    return secret_key;
  }

  @Override
  public String getDefaultKey() {
    return "default";
  }
  
  @Override
  public String[] getKeys() {
    return new String[] { "default" };
  }
}
