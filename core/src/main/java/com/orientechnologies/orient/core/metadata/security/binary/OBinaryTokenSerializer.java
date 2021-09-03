package com.orientechnologies.orient.core.metadata.security.binary;

import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.security.jwt.OBinaryTokenPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenHeader;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenMetaInfo;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenPayload;
import com.orientechnologies.orient.core.metadata.security.jwt.OTokenPayloadDeserializer;
import com.orientechnologies.orient.core.metadata.security.jwt.OrientJwtHeader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class OBinaryTokenSerializer implements OTokenMetaInfo {

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

  private OTokenPayloadDeserializer getForType(String type) {
    switch (type) {
        // The "node" token is for backward compatibility for old distributed binary, may be removed
        // if we do not support runtime compatibility with 3.1 or less
      case "node":
        return new ODistributedBinaryTokenPayloadDeserializer();
      case "OrientDB":
        return new OBinaryTokenPayloadDeserializer();
    }
    throw new ODatabaseException("Unknown payload type");
  }

  public OBinaryTokenSerializer() {
    this(
        new String[] {"plocal", "memory"},
        new String[] {"dafault"},
        new String[] {"HmacSHA256"},
        new String[] {"OrientDB", "node"});
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
    token.setHeader(header);

    OBinaryTokenPayload payload = getForType(header.getType()).deserialize(input, this);
    token.setPayload(payload);

    return token;
  }

  protected static String readString(DataInputStream input) throws IOException {
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
    OTokenPayload payload = token.getPayload();
    assert header.getType() == payload.getPayloadType();
    output.writeByte(associetedTypes.get(header.getType())); // type
    output.writeByte(associetedKeys.get(header.getKeyId())); // keys
    output.writeByte(associetedAlgorithms.get(header.getAlgorithm())); // algorithm
    payload.serialize(output, this);
  }

  public static void writeString(DataOutputStream output, String toWrite)
      throws UnsupportedEncodingException, IOException {
    if (toWrite == null) output.writeShort(-1);
    else {
      byte[] str = toWrite.getBytes("UTF-8");
      output.writeShort(str.length);
      output.write(str);
    }
  }

  @Override
  public String getDbType(int pos) {
    return dbTypes[pos];
  }

  @Override
  public int getDbTypeID(String databaseType) {
    return associetedDdTypes.get(databaseType);
  }
}
