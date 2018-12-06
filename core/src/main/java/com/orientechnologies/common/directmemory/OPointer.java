package com.orientechnologies.common.directmemory;

import com.sun.jna.Pointer;

import java.nio.ByteBuffer;
import java.util.Objects;

public final class OPointer {
  private final Pointer    pointer;
  private final int        size;
  private       ByteBuffer byteBuffer;

  OPointer(Pointer pointer, int size) {
    this.pointer = pointer;
    this.size = size;
  }

  public void clear() {
    pointer.setMemory(0, size, (byte) 0);
  }

  public ByteBuffer getNativeByteBuffer() {
    if (byteBuffer != null) {
      return byteBuffer;
    }

    byteBuffer = pointer.getByteBuffer(0, size);
    return byteBuffer;
  }

  Pointer getNativePointer() {
    return pointer;
  }

  int getSize() {
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;
    OPointer other = (OPointer) o;
    return size == other.size && Objects.equals(pointer, other.pointer);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pointer, size);
  }
}
