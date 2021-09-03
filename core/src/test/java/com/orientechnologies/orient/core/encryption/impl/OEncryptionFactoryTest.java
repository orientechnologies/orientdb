package com.orientechnologies.orient.core.encryption.impl;

import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class OEncryptionFactoryTest {

  @Test
  public void test() {
    String key =
        new String(
            Base64.getEncoder()
                .encode(new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8}));
    OEncryption enc = OEncryptionFactory.INSTANCE.getEncryption(OAESGCMEncryption.NAME, key);
    Assert.assertEquals(OAESGCMEncryption.class, enc.getClass());
  }
}
