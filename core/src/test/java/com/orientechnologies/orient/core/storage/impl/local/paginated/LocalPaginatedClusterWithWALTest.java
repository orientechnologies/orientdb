package com.orientechnologies.orient.core.storage.impl.local.paginated;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.O2QCache;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
@Test
public class LocalPaginatedClusterWithWALTest extends LocalPaginatedClusterTest {
  private OWriteAheadLog writeAheadLog;

  @BeforeClass
  @Override
  public void beforeClass() throws IOException {
    System.out.println("Start LocalPaginatedClusterWithWALTest");
    buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/localPaginatedClusterWithWALTest";

    OLocalPaginatedStorage storage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    when(storage.getStoragePath()).thenReturn(buildDirectory);
    when(storage.getName()).thenReturn("localPaginatedClusterWithWALTest");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    writeAheadLog = new OWriteAheadLog(100 * 1024 * 1024, -1, 10L * 1024 * 1024 * 1024, 100L * 1024 * 1024 * 1024, storage);

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    diskCache = new O2QCache(2L * 1024 * 1024 * 1024, 15000, directMemory, writeAheadLog,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, storage, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(buildDirectory);

    when(storage.getDiskCache()).thenReturn(diskCache);
    when(storage.getWALInstance()).thenReturn(writeAheadLog);
    when(storage.getVariableParser()).thenReturn(variableParser);
    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    paginatedCluster.configure(storage, 5, "localPaginatedClusterWithWALTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  @AfterClass
  @Override
  public void afterClass() throws IOException {
    diskCache.clear();
    writeAheadLog.delete();

    paginatedCluster.delete();
    File file = new File(buildDirectory);
    file.delete();

    System.out.println("End LocalPaginatedClusterWithWALTest");
  }

}
