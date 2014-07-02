package com.orientechnologies.orient.core.serialization.serializer.record.binary;


public class BytesContainer {

  public byte[] bytes;
  public int    offset;

  public BytesContainer(byte[] iSource) {
    bytes = iSource;
  }

  public BytesContainer() {
    bytes = new byte[1024];
  }

  public BytesContainer(byte[] iBytes, short valuePos) {
    this.bytes = iBytes;
    this.offset = valuePos;
  }

  public int alloc(int toAlloc) {
    final int cur = offset;
    offset += toAlloc;
    if (bytes.length < offset)
      resize();
    return cur;
  }

  public void skip(final int read) {
    offset += read;
  }

  public byte[] fitBytes() {
    final byte[] fitted = new byte[offset];
    System.arraycopy(bytes, 0, fitted, 0, offset);
    return fitted;
  }

  private void resize() {
    int newLength = bytes.length;
    while (newLength < offset)
      newLength *= 2;
    final byte[] newBytes = new byte[newLength];
    System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
    bytes = newBytes;
  }

}
