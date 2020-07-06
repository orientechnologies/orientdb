package com.orientechnologies.common.directmemory;

import com.kenai.jffi.MemoryIO;
import java.lang.ref.WeakReference;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

public final class OPointer {

  private final long pointer;
  private final int size;

  private WeakReference<ByteBuffer> byteBuffer;
  private int hash = 0;

  OPointer(long pointer, int size) {
    this.pointer = pointer;
    this.size = size;
  }

  public void clear() {
    MemoryIO.getInstance().setMemory(pointer, size, (byte) 0);
  }

  public ByteBuffer getNativeByteBuffer() {
    ByteBuffer buffer;
    if (byteBuffer == null) {
      buffer = createNativeBuffer();
      byteBuffer = new WeakReference<>(buffer);
    } else {
      buffer = byteBuffer.get();
      if (buffer == null) {
        buffer = createNativeBuffer();
        byteBuffer = new WeakReference<>(buffer);
      }
    }

    return buffer;
  }

  long getNativePointer() {
    return pointer;
  }

  int getSize() {
    return size;
  }

  private ByteBuffer createNativeBuffer() {
    return MemoryIO.getInstance().newDirectByteBuffer(pointer, size).order(ByteOrder.nativeOrder());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }

    OPointer pointer1 = (OPointer) o;

    if (pointer != pointer1.pointer) {
      return false;
    }
    return size == pointer1.size;
  }

  @Override
  public int hashCode() {
    if (hash != 0) {
      return hash;
    }

    int result = (int) (pointer ^ (pointer >>> 32));
    result = 31 * result + size;

    hash = result;
    return hash;
  }
}
