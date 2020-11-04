package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.server.binary.impl.OBinaryTokenPayload;
import com.orientechnologies.orient.server.binary.impl.OTokenPayload;
import java.io.DataInputStream;
import java.io.IOException;

public class OBinaryTokenPayloadDeserializer implements OTokenPayloadDeserializer {

  @Override
  public OTokenPayload deserialize(DataInputStream input, OBinaryTokenSerializer base)
      throws IOException {
    OBinaryTokenPayload payload = new OBinaryTokenPayload();

    payload.setDatabase(OBinaryTokenSerializer.readString(input));
    byte pos = input.readByte();
    if (pos >= 0) {
      payload.setDatabaseType(base.getDbType(pos));
    }

    short cluster = input.readShort();
    long position = input.readLong();
    if (cluster != -1 && position != -1) {
      payload.setUserRid(new ORecordId(cluster, position));
    }
    payload.setExpiry(input.readLong());
    payload.setServerUser(input.readBoolean());
    if (payload.isServerUser()) {
      payload.setUserName(OBinaryTokenSerializer.readString(input));
    }
    payload.setProtocolVersion(input.readShort());
    payload.setSerializer(OBinaryTokenSerializer.readString(input));
    payload.setDriverName(OBinaryTokenSerializer.readString(input));
    payload.setDriverVersion(OBinaryTokenSerializer.readString(input));
    return payload;
  }
}
