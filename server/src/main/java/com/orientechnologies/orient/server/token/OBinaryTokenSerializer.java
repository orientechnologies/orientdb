package com.orientechnologies.orient.server.token;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.server.binary.impl.OBinaryToken;
import com.orientechnologies.orient.server.binary.impl.OBinaryTokenPayload;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class OBinaryTokenSerializer {

  private final String[] types;
  private final String[] keys;
  private final String[] algorithms;
  private final String[] dbTypes;
  private final Map<String, Byte> associetedDdTypes;
  private final Map<String, Byte> associetedKeys;
  private final Map<String, Byte> associetedAlgorithms;
  private final Map<String, Byte> associetedTypes;

  public OBinaryTokenSerializer(
      String[] dbTypes, String[] keys, String[] algorithms, String[] entityTypes) {
    this.dbTypes = dbTypes;
    this.keys = keys;
    this.algorithms = algorithms;
    this.types = entityTypes;
    associetedDdTypes = createMap(dbTypes);
    associetedKeys = createMap(keys);
    associetedAlgorithms = createMap(algorithms);
    associetedTypes = createMap(entityTypes);
  }

  public Map<String, Byte> createMap(String[] entries) {
    Map<String, Byte> newMap = new HashMap<String, Byte>();
    for (int i = 0; i < entries.length; i++) newMap.put(entries[i], (byte) i);
    return newMap;
  }

  public OBinaryToken deserialize(InputStream stream) throws IOException {
    DataInputStream input = new DataInputStream(stream);

    OrientJwtHeader header = new OrientJwtHeader();
    header.setType(types[input.readByte()]);
    header.setKeyId(keys[input.readByte()]);
    header.setAlgorithm(algorithms[input.readByte()]);

    OBinaryToken token = new OBinaryToken();
    OBinaryTokenPayload payload = new OBinaryTokenPayload();
    token.setHeader(header);

    payload.setDatabase(readString(input));
    byte pos = input.readByte();
    if (pos >= 0) {
      payload.setDatabaseType(dbTypes[pos]);
    }

    short cluster = input.readShort();
    long position = input.readLong();
    if (cluster != -1 && position != -1) {
      payload.setUserRid(new ORecordId(cluster, position));
    }
    payload.setExpiry(input.readLong());
    payload.setServerUser(input.readBoolean());
    if (payload.isServerUser()) {
      payload.setUserName(readString(input));
    }
    payload.setProtocolVersion(input.readShort());
    payload.setSerializer(readString(input));
    payload.setDriverName(readString(input));
    payload.setDriverVersion(readString(input));
    token.setPayload(payload);

    return token;
  }

  private String readString(DataInputStream input) throws IOException {
    short s = input.readShort();
    if (s >= 0) {
      byte[] str = new byte[s];
      input.readFully(str);
      return new String(str, "UTF-8");
    }
    return null;
  }

  public void serialize(OBinaryToken token, OutputStream stream) throws IOException {

    DataOutputStream output = new DataOutputStream(stream);
    OTokenHeader header = token.getHeader();
    output.writeByte(associetedTypes.get(header.getType())); // type
    output.writeByte(associetedKeys.get(header.getKeyId())); // keys
    output.writeByte(associetedAlgorithms.get(header.getAlgorithm())); // algorithm

    String toWrite = token.getPayload().getDatabase();
    writeString(output, toWrite);
    if (token.getPayload().getDatabaseType() == null) output.writeByte(-1);
    else output.writeByte(associetedDdTypes.get(token.getDatabaseType()));
    ORID id = token.getPayload().getUserRid();
    if (id == null) {
      output.writeShort(-1);
      output.writeLong(-1);
    } else {
      output.writeShort(id.getClusterId());
      output.writeLong(id.getClusterPosition());
    }
    output.writeLong(token.getPayload().getExpiry());
    output.writeBoolean(token.getPayload().isServerUser());
    if (token.isServerUser()) {
      writeString(output, token.getPayload().getUserName());
    }
    output.writeShort(token.getPayload().getProtocolVersion());
    writeString(output, token.getPayload().getSerializer());
    writeString(output, token.getPayload().getDriverName());
    writeString(output, token.getPayload().getDriverVersion());
  }

  private void writeString(DataOutputStream output, String toWrite)
      throws UnsupportedEncodingException, IOException {
    if (toWrite == null) output.writeShort(-1);
    else {
      byte[] str = toWrite.getBytes("UTF-8");
      output.writeShort(str.length);
      output.write(str);
    }
  }
}
