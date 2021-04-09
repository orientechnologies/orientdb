package com.orientechnologies.orient.core.metadata.security.binary;

import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.jwt.OBinaryTokenPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenMetaInfo;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenPayloadDeserializer;
import java.io.DataInputStream;
import java.io.IOException;

public class OBinaryTokenPayloadDeserializer implements OTokenPayloadDeserializer {

  @Override
  public OBinaryTokenPayload deserialize(DataInputStream input, OTokenMetaInfo base)
      throws IOException {
    OBinaryTokenPayloadImpl payload = new OBinaryTokenPayloadImpl();

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
