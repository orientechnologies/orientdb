package com.orientechnologies.orient.core.index.hashindex.local.arc;

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.O2QCache;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Artem Loginov(logart)
 */
public class O2QCacheSupportFunctionTest {
  private ODirectMemory directMemory;
  private OStorageLocal storageLocal;

  @BeforeClass
  public void beforeClass() throws IOException {
    directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OStorageLocal) Orient.instance().loadStorage("local:" + buildDirectory + "/O2QCacheTest");

    OStorageSegmentConfiguration fileConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTest", 0);
    fileConfiguration.fileType = OFileFactory.CLASSIC;
    fileConfiguration.fileMaxSize = "10000Mb";
  }

  @Test
  public void testCacheShouldCreateFileIfItIsNotExisted() throws Exception {
    O2QCache cache = new O2QCache(32, directMemory, 8, storageLocal, true);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTestUnique", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;

    cache.openFile(segmentConfiguration, ".tst");

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTestUnique.0.tst");

    Assert.assertTrue(file.exists());
    Assert.assertTrue(file.isFile());
  }
}
