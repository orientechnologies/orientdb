package com.orientechnologies.orient.core.metadata.security.jwt;

import java.io.DataInputStream;
import java.io.IOException;

public interface OTokenPayloadDeserializer {

  public OBinaryTokenPayload deserialize(DataInputStream input, OTokenMetaInfo base)
      throws IOException;
}
