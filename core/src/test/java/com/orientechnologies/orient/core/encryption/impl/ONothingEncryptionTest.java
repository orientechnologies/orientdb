package com.orientechnologies.orient.core.encryption.impl;

import org.junit.Assert;
import org.junit.Test;

import java.util.Base64;

public class ONothingEncryptionTest {

  @Test
  public void test() {
    ONothingEncryption encryption = new ONothingEncryption();
    String key = new String(Base64.getEncoder().encode(new byte[]{1,2,3,4,5,6,7,8,1,2,3,4,5,6,7,8}));
    encryption.configure(key);

    byte[] original = "this is a test string to encrypt".getBytes();
    byte[] encrypted = encryption.encrypt(original);
    Assert.assertArrayEquals(original, encryption.decrypt(encrypted));
  }
}
