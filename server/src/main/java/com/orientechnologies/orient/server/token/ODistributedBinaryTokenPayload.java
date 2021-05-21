package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.server.binary.impl.OBinaryTokenPayloadImpl;

// The "node" token is for backward compatibility for old distributed binary, may be removed if
// we do not support runtime compatibility with 3.1 or less
public class ODistributedBinaryTokenPayload extends OBinaryTokenPayloadImpl {

  @Override
  public String getPayloadType() {
    return "node";
  }
}
