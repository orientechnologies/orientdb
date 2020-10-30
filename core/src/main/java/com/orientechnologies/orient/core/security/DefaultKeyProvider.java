package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.metadata.security.jwt.OKeyProvider;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import java.security.Key;
import javax.crypto.spec.SecretKeySpec;

/**
 * Created by emrul on 28/09/2014.
 *
 * @author Emrul Islam <emrul@emrul.com> Copyright 2014 Emrul Islam
 */
public class DefaultKeyProvider implements OKeyProvider {

  private SecretKeySpec secretKey;

  public DefaultKeyProvider(byte[] secret) {
    secretKey = new SecretKeySpec(secret, "HmacSHA256");
  }

  @Override
  public Key getKey(OTokenHeader header) {
    return secretKey;
  }

  @Override
  public String getDefaultKey() {
    return "default";
  }

  @Override
  public String[] getKeys() {
    return new String[] {"default"};
  }
}
