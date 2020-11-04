package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.server.binary.impl.OTokenPayload;
import java.io.DataInputStream;
import java.io.IOException;

public interface OTokenPayloadDeserializer {

  public OTokenPayload deserialize(DataInputStream input, OBinaryTokenSerializer base)
      throws IOException;
}
