package com.orientechnologies.orient.core.storage.ridbag.sbtree;

import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;

public interface Change {
  int SIZE = OByteSerializer.BYTE_SIZE + OIntegerSerializer.INT_SIZE;

  void increment();

  void decrement();

  int applyTo(Integer value);

  int getValue();

  byte getType();

  /**
   * Checks if put increment operation can be safely performed.
   *
   * @return true if increment operation can be safely performed.
   */
  boolean isUndefined();

  void applyDiff(int delta);

  int serialize(byte[] stream, int offset);
}
