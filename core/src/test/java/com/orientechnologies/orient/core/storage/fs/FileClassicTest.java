package com.orientechnologies.orient.core.storage.fs;

import java.io.File;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;

/**
 * @author Andrey Lomakin
 * @since 5/5/13
 */
@Test
public class FileClassicTest {

  private String buildDirectory;

  @BeforeClass
  public void beforeClass() {
    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";
  }

  public void testSoftlyClosedFailed() throws Exception {
    OFileClassic fileClassicOne = new OFileClassic(buildDirectory + File.separator + "file.tst", "rw");
    fileClassicOne.create();

    Assert.assertFalse(fileClassicOne.isSoftlyClosed());
    Assert.assertTrue(fileClassicOne.wasSoftlyClosed());

    fileClassicOne.allocateSpace(OIntegerSerializer.INT_SIZE);
    fileClassicOne.writeInt(0, 12);

    OFileClassic fileClassicTwo = new OFileClassic(buildDirectory + File.separator + "file.tst", "rw");
    Assert.assertFalse(fileClassicTwo.open());

    Assert.assertFalse(fileClassicTwo.isSoftlyClosed());
    Assert.assertFalse(fileClassicTwo.wasSoftlyClosed());

    fileClassicTwo.close();
    fileClassicOne.delete();
  }

  public void testSoftlyClosedSuccess() throws Exception {
    OFileClassic fileClassicOne = new OFileClassic(buildDirectory + File.separator + "file.tst", "rw");
    fileClassicOne.create();

    Assert.assertFalse(fileClassicOne.isSoftlyClosed());
    Assert.assertTrue(fileClassicOne.wasSoftlyClosed());

    fileClassicOne.allocateSpace(OIntegerSerializer.INT_SIZE);
    fileClassicOne.writeInt(0, 12);
    fileClassicOne.close();

    OFileClassic fileClassicTwo = new OFileClassic(buildDirectory + File.separator + "file.tst", "rw");
    Assert.assertTrue(fileClassicTwo.open());

    Assert.assertFalse(fileClassicTwo.isSoftlyClosed());
    Assert.assertTrue(fileClassicTwo.wasSoftlyClosed());

    fileClassicTwo.close();
    fileClassicTwo.delete();
  }
}
