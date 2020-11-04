package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.server.binary.impl.OBinaryTokenPayloadImpl;

public class ODistributedBinaryTokenPayload extends OBinaryTokenPayloadImpl {

  @Override
  public String getPayloadType() {
    return "node";
  }
}
