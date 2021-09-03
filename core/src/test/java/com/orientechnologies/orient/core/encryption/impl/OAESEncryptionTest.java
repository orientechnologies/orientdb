package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.orient.core.exception.OSecurityException;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class OAESEncryptionTest {

  @Test(expected = OSecurityException.class)
  public void testNotInited() {
    OAESEncryption encryption = new OAESEncryption();
    byte[] original = "this is a test string to encrypt".getBytes();
    encryption.encrypt(original);
  }

  @Test
  public void test() {
    OAESEncryption encryption = new OAESEncryption();
    String key =
        new String(
            Base64.getEncoder()
                .encode(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8}));
    encryption.configure(key);

    byte[] original = "this is a test string to encrypt".getBytes();
    byte[] encrypted = encryption.encrypt(original);
    Assert.assertArrayEquals(original, encryption.decrypt(encrypted));
  }
}
