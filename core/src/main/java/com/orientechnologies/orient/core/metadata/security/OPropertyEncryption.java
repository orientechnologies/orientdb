package com.orientechnologies.orient.core.metadata.security;

public interface OPropertyEncryption {

  boolean isEncrypted(String name);

  byte[] encrypt(String name, byte[] values);

  byte[] decrypt(String name, byte[] values);
}
