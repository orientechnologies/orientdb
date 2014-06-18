package com.orientechnologies.orient.core.serialization.serializer.record.binary;

public class BytesContainer {

  public byte[] bytes;
  public short  offset;

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

  public short alloc(short toAlloc) {
    short cur = offset;
    offset += toAlloc;
    if (bytes.length < offset)
      resize();
    return cur;
  }

  private void resize() {
    int newLength = bytes.length;
    while (newLength < offset)
      newLength *= 2;
    byte[] newBytes = new byte[newLength];
    System.arraycopy(bytes, 0, newBytes, 0, bytes.length);
    bytes = newBytes;
  }

  public void read(int read) {
    offset += read;
  }

}
