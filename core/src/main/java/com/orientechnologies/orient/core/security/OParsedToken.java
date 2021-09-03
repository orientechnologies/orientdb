package com.orientechnologies.orient.core.security;

import com.orientechnologies.orient.core.metadata.security.OToken;

public class OParsedToken {

  private OToken token;
  private byte[] tokenBytes;
  private byte[] signature;

  public OParsedToken(OToken token, byte[] tokenBytes, byte[] signature) {
    super();
    this.token = token;
    this.tokenBytes = tokenBytes;
    this.signature = signature;
  }

  public OToken getToken() {
    return token;
  }

  public byte[] getTokenBytes() {
    return tokenBytes;
  }

  public byte[] getSignature() {
    return signature;
  }
}
