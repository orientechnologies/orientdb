package com.orientechnologies.orient.server.token;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.jwt.OJwtHeader;
import com.orientechnologies.orient.server.binary.impl.OBinaryToken;

public class OBinaryTokenSerializer {

  private final String[]          types;
  private final String[]          keys;
  private final String[]          algorithms;
  private final String[]          dbTypes;
  private final Map<String, Byte> associetedDdTypes;
  private final Map<String, Byte> associetedKeys;
  private final Map<String, Byte> associetedAlgorithms;
  private final Map<String, Byte> associetedTypes;

  public OBinaryTokenSerializer(String dbTypes[], String keys[], String algorithms[], String entityTypes[]) {
    this.dbTypes = dbTypes;
    this.keys = keys;
    this.algorithms = algorithms;
    this.types = entityTypes;
    associetedDdTypes = createMap(dbTypes);
    associetedKeys = createMap(keys);
    associetedAlgorithms = createMap(algorithms);
    associetedTypes = createMap(entityTypes);
  }

  public Map<String, Byte> createMap(String entries[]) {
    Map<String, Byte> newMap = new HashMap<String, Byte>();
    for (int i = 0; i < entries.length; i++)
      newMap.put(entries[i], (byte) i);
    return newMap;
  }

  public OBinaryToken deserialize(InputStream stream) throws IOException {
    DataInputStream input = new DataInputStream(stream);

    OrientJwtHeader header = new OrientJwtHeader();
    header.setType(types[input.readByte()]);
    header.setKeyId(keys[input.readByte()]);
    header.setAlgorithm(algorithms[input.readByte()]);

    OBinaryToken token = new OBinaryToken();
    token.setHeader(header);

    token.setDatabase(readString(input));
    byte pos = input.readByte();
    if (pos >= 0)
      token.setDatabaseType(dbTypes[pos]);

    short cluster = input.readShort();
    long position = input.readLong();
    if (cluster != -1 && position != -1)
      token.setUserRid(new ORecordId(cluster, position));
    token.setExpiry(input.readLong());
    token.setServerUser(input.readBoolean());
    if (token.isServerUser()) {
      token.setUserName(readString(input));
    }

    token.setProtocolVersion(input.readShort());
    token.setSerializer(readString(input));
    token.setDriverName(readString(input));
    token.setDriverVersion(readString(input));

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
    OJwtHeader header = token.getHeader();
    output.writeByte(associetedTypes.get(header.getType()));// type
    output.writeByte(associetedKeys.get(header.getKeyId()));// keys
    output.writeByte(associetedAlgorithms.get(header.getAlgorithm()));// algorithm

    String toWrite = token.getDatabase();
    writeString(output, toWrite);
    if (token.getDatabaseType() == null)
      output.writeByte(-1);
    else
      output.writeByte(associetedDdTypes.get(token.getDatabaseType()));
    ORID id = token.getUserId();
    if (id == null) {
      output.writeShort(-1);
      output.writeLong(-1);
    } else {
      output.writeShort(id.getClusterId());
      output.writeLong(id.getClusterPosition());
    }
    output.writeLong(token.getExpiry());
    output.writeBoolean(token.isServerUser());
    if (token.isServerUser()) {
      writeString(output, token.getUserName());
    }
    output.writeShort(token.getProtocolVersion());
    writeString(output, token.getSerializer());
    writeString(output, token.getDriverName());
    writeString(output, token.getDriverVersion());

  }

  private void writeString(DataOutputStream output, String toWrite) throws UnsupportedEncodingException, IOException {
    if (toWrite == null)
      output.writeShort(-1);
    else {
      byte[] str = toWrite.getBytes("UTF-8");
      output.writeShort(str.length);
      output.write(str);
    }
  }
}
