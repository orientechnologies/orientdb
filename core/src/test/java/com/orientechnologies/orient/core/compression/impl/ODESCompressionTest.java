package com.orientechnologies.orient.core.compression.impl;

import com.orientechnologies.orient.core.compression.OCompressionFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OSecurityException;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * @author giastfader@github
 * @since 22.04.2015
 */
@Test
public class ODESCompressionTest extends AbstractCompressionTest {
  public void testODESEncryptedCompressionNoKey() {
    OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.setValue(null);
    try {
      testCompression(ODESCompression.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testODESEncryptedCompressionInvalidKey() {
    OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.setValue("A");
    try {
      OCompressionFactory.INSTANCE.getCompression(ODESCompression.NAME);
      testCompression(ODESCompression.NAME);
      Assert.fail();
    } catch (OSecurityException e) {
    }
  }

  public void testODESEncryptedCompression() {
    OGlobalConfiguration.STORAGE_ENCRYPTION_AES_KEY.setValue("T1JJRU5UREJfSVNfQ09PTA==");
    OGlobalConfiguration.STORAGE_ENCRYPTION_DES_KEY.setValue("T1JJRU5UREI=");
    OCompressionFactory.INSTANCE.getCompression(ODESCompression.NAME);
    testCompression(ODESCompression.NAME);
  }
}
