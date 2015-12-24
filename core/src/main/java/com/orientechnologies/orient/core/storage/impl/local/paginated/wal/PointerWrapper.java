package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.directmemory.ODirectMemoryPointer;

/**
 * Created by tglman on 24/12/15.
 */
public final class PointerWrapper {
  private       OWALChanges          owalChanges;
  private final ODirectMemoryPointer pointer;

  PointerWrapper(OWALChanges owalChanges, ODirectMemoryPointer pointer) {
    this.owalChanges = owalChanges;
    this.pointer = pointer;
  }

  public byte getByte(long offset) {
    return owalChanges.getByteValue(pointer, (int) offset);
  }

  public short getShort(long offset) {
    return owalChanges.getShortValue(pointer, (int) offset);
  }

  public int getInt(long offset) {
    return owalChanges.getIntValue(pointer, (int) offset);
  }

  public long getLong(long offset) {
    return owalChanges.getLongValue(pointer, (int) offset);
  }

  public byte[] get(long offset, int len) {
    return owalChanges.getBinaryValue(pointer, (int) offset, len);
  }

  public char getChar(long offset) {
    return (char) owalChanges.getShortValue(pointer, (int) offset);
  }

}
