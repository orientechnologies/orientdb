package com.orientechnologies.orient.core.serialization.serializer.record.binary;

public class BytesContainer {

  public byte[] bytes;
  public short  offset;

  public BytesContainer(byte[] iSource) {
    bytes = iSource;
  }

  public BytesContainer() {
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
    // TODO Auto-generated method stub

  }

  public void read(int read) {
    offset += read;
  }

}
