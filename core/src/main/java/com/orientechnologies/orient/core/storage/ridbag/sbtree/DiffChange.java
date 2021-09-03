package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

public class DiffChange implements Change {
  public static final byte TYPE = 0;
  private int delta;

  public DiffChange(int delta) {
    this.delta = delta;
  }

  @Override
  public void increment() {
    delta++;
  }

  @Override
  public void decrement() {
    delta--;
  }

  @Override
  public int applyTo(Integer value) {
    int result;
    if (value == null) result = delta;
    else result = value + delta;

    if (result < 0) result = 0;

    return result;
  }

  @Override
  public byte getType() {
    return TYPE;
  }

  @Override
  public int getValue() {
    return delta;
  }

  @Override
  public boolean isUndefined() {
    return delta < 0;
  }

  @Override
  public void applyDiff(int delta) {
    this.delta += delta;
  }

  @Override
  public int serialize(byte[] stream, int offset) {
    OByteSerializer.INSTANCE.serializeLiteral(TYPE, stream, offset);
    OIntegerSerializer.INSTANCE.serializeLiteral(delta, stream, offset + OByteSerializer.BYTE_SIZE);
    return OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE;
  }
}
