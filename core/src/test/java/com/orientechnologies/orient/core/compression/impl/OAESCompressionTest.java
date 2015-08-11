package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author giastfader@github
 * @since 22.04.2015
 */
@Test
public class OAESCompressionTest extends AbstractCompressionTest {
  public void testOAESEncryptedCompressionNoKey() {
    OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.setValue("T1JJRU5UREI=");
    OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.setValue(null);
    try {
      testCompression(OAESCompression.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testOAESEncryptedCompressionInvalidKey() {
    OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.setValue("T1JJRU5UREI=");
    OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.setValue("A");
    try {
      testCompression(OAESCompression.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testOAESEncryptedCompression() {
    OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.setValue("T1JJRU5UREI=");
    OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    testCompression(OAESCompression.NAME);
  }
}
