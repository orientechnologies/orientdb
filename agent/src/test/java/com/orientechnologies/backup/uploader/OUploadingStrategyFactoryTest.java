package com.orientechnologies.backup.uploader;

import com.orientechnologies.orient.core.exception.OConfigurationException;
import org.junit.Assert;
import org.junit.Test;

public class OUploadingStrategyFactoryTest {

  @Test
  public void test() {
    OUploadingStrategyFactory factory = new OUploadingStrategyFactory();
    Assert.assertTrue(factory.buildStrategy("s3") instanceof OS3DeltaUploadingStrategy);
    Assert.assertTrue(factory.buildStrategy("sftp") instanceof OSFTPDeltaUploadingStrategy);
    try {
      factory.buildStrategy("foo");
      Assert.fail();
    } catch (OConfigurationException e) {

    }
  }
}
