package com.orientechnologies.common.directmemory;

import com.sun.jna.Pointer;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.util.Objects;

public final class OPointer {

  private final Pointer pointer;
  private final int size;
  private WeakReference<ByteBuffer> byteBuffer;
  private int hash = 0;

  OPointer(Pointer pointer, int size) {
    this.pointer = pointer;
    this.size = size;
  }

  public void clear() {
    pointer.setMemory(0, size, (byte) 0);
  }

  public ByteBuffer getNativeByteBuffer() {
    ByteBuffer buffer;

    if (byteBuffer == null) {
      buffer = pointer.getByteBuffer(0, size);
      byteBuffer = new WeakReference<>(buffer);
    } else {
      buffer = byteBuffer.get();
      if (buffer == null) {
        buffer = pointer.getByteBuffer(0, size);
        byteBuffer = new WeakReference<>(buffer);
      }
    }

    return buffer;
  }

  Pointer getNativePointer() {
    return pointer;
  }

  int getSize() {
    return size;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    final OPointer otherPointer = (OPointer) o;

    if (size != otherPointer.size) {
      return false;
    }

    return Objects.equals(pointer, otherPointer.pointer);
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }

    @SuppressWarnings("ConditionalCanBeOptional")
    int result = pointer != null ? pointer.hashCode() : 0;
    result = 31 * result + size;

    hash = result;

    return hash;
  }
}
