package com.orientechnologies.orient.core.db;

public class OStringCacheKey {

  private final byte[] bytes;
  private final int offset;
  private final int len;
  private int hash;

  public OStringCacheKey(byte[] bytes, int offset, int len) {
    this.bytes = bytes;
    this.offset = offset;
    this.len = len;
  }

  public int hashCode() {
    int h = hash;
    if (h == 0 && len > 0) {
      int finalLen = offset + len;
      for (int i = offset; i < finalLen; i++) {
        h = 31 * h + this.bytes[i];
      }
      hash = h;
    }
    return h;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj instanceof OStringCacheKey) {
      OStringCacheKey sobj = (OStringCacheKey) obj;
      if (sobj.len != this.len) {
        return false;
      }
      int finalLen = this.offset + this.len;
      for (int c1 = this.offset, c2 = sobj.offset; c1 < finalLen; c1++, c2++) {
        if (this.bytes[c1] != sobj.bytes[c2]) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
