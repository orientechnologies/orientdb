package com.orientechnologies.common.directmemory;

import com.sun.jna.Pointer;

import java.nio.ByteBuffer;

public final class OPointer {
  private final Pointer    pointer;
  private final int        size;
  private final ByteBuffer byteBuffer;
  private       int        hash = 0;

  OPointer(Pointer pointer, int size) {
    this.pointer = pointer;
    this.size = size;
    this.byteBuffer = pointer.getByteBuffer(0, size);
  }

  public void clear() {
    pointer.setMemory(0, size, (byte) 0);
  }

  public ByteBuffer getNativeByteBuffer() {
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

    OPointer pointer1 = (OPointer) o;

    if (size != pointer1.size)
      return false;
    return pointer != null ? pointer.equals(pointer1.pointer) : pointer1.pointer == null;
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }

    int result = pointer != null ? pointer.hashCode() : 0;
    result = 31 * result + size;

    hash = result;

    return hash;
  }
}
