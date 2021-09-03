package com.orientechnologies.orient.server;

import java.util.Arrays;

public class OHashToken {

  private byte[] binaryToken;

  public OHashToken(byte[] binaryToken) {
    this.binaryToken = binaryToken;
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(binaryToken);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (!(obj instanceof OHashToken)) return false;
    return Arrays.equals(this.binaryToken, ((OHashToken) obj).binaryToken);
  }
}
