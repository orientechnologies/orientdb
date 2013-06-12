package com.orientechnologies.orient.core.storage.impl.local.paginated;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.O2QCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.storage.fs.OAbstractFile;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractPageWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OClusterStateRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
@Test
public class LocalPaginatedClusterWithWAL extends LocalPaginatedClusterTest {
  private OWriteAheadLog         writeAheadLog;

  private OLocalPaginatedCluster testCluster;
  private ODiskCache             testDiskCache;
  private OLocalPaginatedStorage testStorage;

  @BeforeMethod
  @Override
  public void beforeMethod() throws IOException {
    buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/localPaginatedClusterWithWALTest";

    createPaginatedCluster();
    createTestPaginatedCluster();
  }

  private void createPaginatedCluster() throws IOException {
    OLocalPaginatedStorage storage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    when(storage.getStoragePath()).thenReturn(buildDirectory);
    when(storage.getName()).thenReturn("localPaginatedClusterWithWALTest");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    writeAheadLog = new OWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, 100L * 1024 * 1024 * 1024, storage);

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    diskCache = new O2QCache(1024L * 1024 * 1024 * 1024, 15000, directMemory, writeAheadLog,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, storage, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(buildDirectory);

    when(storage.getDiskCache()).thenReturn(diskCache);
    when(storage.getWALInstance()).thenReturn(writeAheadLog);
    when(storage.getVariableParser()).thenReturn(variableParser);
    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    paginatedCluster = new OLocalPaginatedCluster();
    paginatedCluster.configure(storage, 5, "localPaginatedClusterWithWALTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  private void createTestPaginatedCluster() throws IOException {
    testStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    when(testStorage.getStoragePath()).thenReturn(buildDirectory);
    when(testStorage.getName()).thenReturn("localPaginatedClusterWithWALTest");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    testDiskCache = new O2QCache(1024 * 1024 * 1024, 15000, directMemory, null,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, testStorage, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(buildDirectory);

    when(testStorage.getDiskCache()).thenReturn(testDiskCache);
    when(testStorage.getWALInstance()).thenReturn(null);
    when(testStorage.getVariableParser()).thenReturn(variableParser);
    when(testStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(testStorage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(buildDirectory);

    testCluster = new OLocalPaginatedCluster();
    testCluster.configure(testStorage, 6, "testPaginatedClusterWithWALTest", buildDirectory, -1);
    testCluster.create(-1);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    writeAheadLog.delete();
    paginatedCluster.delete();
    diskCache.clear();

    testCluster.delete();
    testDiskCache.clear();

    File file = new File(buildDirectory);
    file.delete();
  }

  @BeforeClass
  @Override
  public void beforeClass() throws IOException {
    System.out.println("Start LocalPaginatedClusterWithWALTest");
  }

  @AfterClass
  @Override
  public void afterClass() throws IOException {
    System.out.println("End LocalPaginatedClusterWithWALTest");
  }

  @Override
  public void testAddOneSmallRecord() throws IOException {
    super.testAddOneSmallRecord();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddOneBigRecord() throws IOException {
    super.testAddOneBigRecord();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateOneSmallRecord() throws IOException {
    super.testUpdateOneSmallRecord();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateOneBigRecord() throws IOException {
    super.testUpdateOneBigRecord();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testDeleteRecordAndAddNewOnItsPlace() throws IOException {
    super.testDeleteRecordAndAddNewOnItsPlace();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddManySmallRecords() throws IOException {
    super.testAddManySmallRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateOneSmallRecordVersionIsLowerCurrentOne() throws IOException {
    super.testUpdateOneSmallRecordVersionIsLowerCurrentOne();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateOneSmallRecordVersionIsMinusTwo() throws IOException {
    super.testUpdateOneSmallRecordVersionIsMinusTwo();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateManySmallRecords() throws IOException {
    super.testUpdateManySmallRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddManyRecords() throws IOException {
    super.testAddManyRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddManyBigRecords() throws IOException {
    super.testAddManyBigRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateManyRecords() throws IOException {
    super.testUpdateManyRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testUpdateManyBigRecords() throws IOException {
    super.testUpdateManyBigRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testRemoveHalfSmallRecords() throws IOException {
    super.testRemoveHalfSmallRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testRemoveHalfRecords() throws IOException {
    super.testRemoveHalfRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testRemoveHalfBigRecords() throws IOException {
    super.testRemoveHalfBigRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testRemoveHalfRecordsAndAddAnotherHalfAgain() throws IOException {
    super.testRemoveHalfRecordsAndAddAnotherHalfAgain();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test(enabled = false)
  public void testForwardIteration() throws IOException {
    super.testForwardIteration();
  }

  @Override
  @Test(enabled = false)
  public void testBackwardIteration() throws IOException {
    super.testBackwardIteration();
  }

  @Override
  @Test(enabled = false)
  public void testGetPhysicalPosition() throws IOException {
    super.testGetPhysicalPosition();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    paginatedCluster.close();
    writeAheadLog.close();

    diskCache.clear();

    restoreClusterFromWAL(testCluster);

    testCluster.close();

    assertClusterContentIsTheSame(paginatedCluster.getName(), testCluster.getName());

    testCluster.open();
    paginatedCluster.open();
  }

  private void restoreClusterFromWAL(OLocalPaginatedCluster testCluster) throws IOException {
    OWriteAheadLog log = new OWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, 100L * 1024 * 1024 * 1024, testStorage);
    OLogSequenceNumber lsn = log.begin();

    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();

    boolean atomicChangeIsProcessed = false;
    while (lsn != null) {
      OWALRecord walRecord = log.read(lsn);
      atomicUnit.add(walRecord);

      if (!atomicChangeIsProcessed) {
        Assert.assertTrue(walRecord instanceof OAtomicUnitStartRecord);
        atomicChangeIsProcessed = true;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        atomicChangeIsProcessed = false;

        for (OWALRecord restoreRecord : atomicUnit) {
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord)
            continue;

          testCluster.restoreRecord(restoreRecord);
        }
        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OAbstractPageWALRecord || walRecord instanceof OClusterStateRecord);
      }

      lsn = log.next(lsn);
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();
  }

  private void assertClusterContentIsTheSame(String expectedCluster, String actualCluster) throws IOException {
    int i = 0;
    File expectedFile = new File(buildDirectory, expectedCluster + "." + i + ".pcl");
    while (expectedFile.exists()) {
      RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r");
      RandomAccessFile fileTwo = new RandomAccessFile(new File(buildDirectory, actualCluster + "." + i + ".pcl"), "r");

      Assert.assertEquals(fileOne.length(), fileTwo.length());

      byte[] expectedContent = new byte[OLocalPage.PAGE_SIZE];
      byte[] actualContent = new byte[OLocalPage.PAGE_SIZE];

      fileOne.seek(OAbstractFile.HEADER_SIZE);
      fileTwo.seek(OAbstractFile.HEADER_SIZE);

      int bytesRead = fileOne.read(expectedContent);
      while (bytesRead >= 0) {
        fileTwo.readFully(actualContent, 0, bytesRead);

        Assert.assertEquals(expectedContent, actualContent);

        expectedContent = new byte[OLocalPage.PAGE_SIZE];
        actualContent = new byte[OLocalPage.PAGE_SIZE];
        bytesRead = fileOne.read(expectedContent);
      }

      fileOne.close();
      fileTwo.close();

      i++;
      expectedFile = new File(buildDirectory, expectedCluster + "." + i + ".pcl");
    }
  }
}
