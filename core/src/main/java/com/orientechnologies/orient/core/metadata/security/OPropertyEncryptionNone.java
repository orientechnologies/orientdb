package com.orientechnologies.orient.core.metadata.security;

public class OPropertyEncryptionNone implements OPropertyEncryption {

  private static final OPropertyEncryptionNone inst = new OPropertyEncryptionNone();

  public static OPropertyEncryption instance() {
    return inst;
  }

  public boolean isEncrypted(String name) {
    return false;
  }

  public byte[] encrypt(String name, byte[] values) {
    return values;
  }

  public byte[] decrypt(String name, byte[] values) {
    return values;
  }
}
