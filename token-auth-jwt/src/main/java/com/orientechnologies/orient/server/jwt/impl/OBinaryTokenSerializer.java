package com.orientechnologies.orient.server.jwt.impl;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
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
    token.setDatabaseType(dbTypes[input.readByte()]);
    short cluster = input.readShort();
    long position = input.readLong();
    token.setUserRid(new ORecordId(cluster, position));
    token.setExpiry(input.readLong());

    return token;
  }

  private String readString(DataInputStream input) throws IOException {
    short s = input.readShort();
    byte[] str = new byte[s];
    input.readFully(str);
    return new String(str, "UTF-8");
  }

  public void serialize(OBinaryToken token, OutputStream stream) throws IOException {

    DataOutputStream output = new DataOutputStream(stream);
    OJwtHeader header = token.getHeader();
    output.writeByte(associetedTypes.get(header.getType()));// type
    output.writeByte(associetedKeys.get(header.getKeyId()));// keys
    output.writeByte(associetedAlgorithms.get(header.getAlgorithm()));// algorithm

    byte[] str = token.getDatabase().getBytes("UTF-8");
    output.writeShort(str.length);
    output.write(str);
    output.writeByte(associetedDdTypes.get(token.getDatabaseType()));
    ORID id = token.getUserId();
    output.writeShort(id.getClusterId());
    output.writeLong(id.getClusterPosition());
    output.writeLong(token.getExpiry());

  }
}
