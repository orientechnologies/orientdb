package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * Created by tglman on 24/12/15.
 */
public final class PointerWrapper {
  private       OWALChanges          changes;
  private final ODirectMemoryPointer pointer;

  PointerWrapper(OWALChanges changes, ODirectMemoryPointer pointer) {
    this.changes = changes;
    this.pointer = pointer;
  }

  public byte getByte(long offset) {
    return changes.getByteValue(pointer, (int) offset);
  }

  public short getShort(long offset) {
    return changes.getShortValue(pointer, (int) offset);
  }

  public int getInt(long offset) {
    return changes.getIntValue(pointer, (int) offset);
  }

  public long getLong(long offset) {
    return changes.getLongValue(pointer, (int) offset);
  }

  public byte[] get(long offset, int len) {
    return changes.getBinaryValue(pointer, (int) offset, len);
  }

  public char getChar(long offset) {
    return (char) changes.getShortValue(pointer, (int) offset);
  }

}
