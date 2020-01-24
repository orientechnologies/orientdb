package com.orientechnologies.orient.core.storage;

import java.io.*;

public interface OTransactionMetadata extends Comparable<OTransactionMetadata> {

  default byte[] serialize() throws IOException {
    ByteArrayOutputStream output = new ByteArrayOutputStream();
    serialize(new DataOutputStream(output));
    return output.toByteArray();
  }

  static OTransactionMetadata deserialize(byte[] data) throws IOException {
    return deserialize(new DataInputStream(new ByteArrayInputStream(data)));
  }

  static OTransactionMetadata deserialize(DataInput input) throws IOException {
    OTransactionMetadata meta = OTransactionMetadataFactory.create(input.readByte());
    meta.deserializeInternal(input);
    return meta;
  }

  default void serialize(DataOutput output) throws IOException {
    output.writeByte(getType());
    serializeInternal(output);
  }

  void serializeInternal(DataOutput output) throws IOException;

  void deserializeInternal(DataInput input) throws IOException;

  byte getType();
}
