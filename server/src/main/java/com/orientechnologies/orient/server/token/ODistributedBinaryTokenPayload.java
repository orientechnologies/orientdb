package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.server.binary.impl.OBinaryTokenPayload;

public class ODistributedBinaryTokenPayload extends OBinaryTokenPayload {

  @Override
  public String getPayloadType() {
    return "node";
  }
}
