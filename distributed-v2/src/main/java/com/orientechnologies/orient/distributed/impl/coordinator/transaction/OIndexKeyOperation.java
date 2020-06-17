package com.orientechnologies.orient.distributed.impl.coordinator.transaction;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class OIndexKeyOperation {
  // Key Change Operation
  public static final byte PUT = 1;
  public static final byte REMOVE = 2;

  private byte type;
  private ORID value;

  public OIndexKeyOperation(byte type, ORID value) {
    this.type = type;
    this.value = value;
  }

  public OIndexKeyOperation() {}

  public void serialize(DataOutput output) throws IOException {
    output.write(type);
    ORecordId.serialize(value, output);
  }

  public void deserialize(DataInput input) throws IOException {
    type = input.readByte();
    value = ORecordId.deserialize(input);
  }

  public byte getType() {
    return type;
  }

  public ORID getValue() {
    return value;
  }
}
