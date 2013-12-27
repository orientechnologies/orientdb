package com.orientechnologies.orient.core.storage.impl.local.paginated;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteDiskCache;
import com.orientechnologies.orient.core.storage.fs.OAbstractFile;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
@Test
public class LocalPaginatedClusterWithWAL extends LocalPaginatedClusterTest {
  private OWriteAheadLog         writeAheadLog;

  private OPaginatedCluster      testCluster;
  private ODiskCache             testDiskCache;
  private OLocalPaginatedStorage testStorage;

  private String                 storageDir;
  private String                 testStorageDir;
  private OLocalPaginatedStorage storage;

  @BeforeMethod
  @Override
  public void beforeMethod() throws IOException {
    buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/localPaginatedClusterWithWALTest";

    createPaginatedCluster();
    createTestPaginatedCluster();
  }

  private void createPaginatedCluster() throws IOException {
    storage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    storageDir = buildDirectory + "/localPaginatedClusterWithWALTestOne";
    when(storage.getStoragePath()).thenReturn(storageDir);
    when(storage.getName()).thenReturn("localPaginatedClusterWithWALTestOne");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File storageDirOneFile = new File(storageDir);
    if (!storageDirOneFile.exists())
      storageDirOneFile.mkdirs();

    writeAheadLog = new OWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, 100L * 1024 * 1024 * 1024, storage);

    diskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 1648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, storage, null, false, false);
    atomicOperationsManager = new OAtomicOperationsManager(writeAheadLog);

    OStorageVariableParser variableParser = new OStorageVariableParser(storageDir);

    when(storage.getStorageTransaction()).thenReturn(null);
    when(storage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);
    when(storage.getDiskCache()).thenReturn(diskCache);
    when(storage.getWALInstance()).thenReturn(writeAheadLog);
    when(storage.getVariableParser()).thenReturn(variableParser);
    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(storageDir);

    paginatedCluster = new OPaginatedCluster();
    paginatedCluster.configure(storage, 5, "localPaginatedClusterWithWALTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  private void createTestPaginatedCluster() throws IOException {
    testStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    testStorageDir = buildDirectory + "/localPaginatedClusterWithWALTestTwo";
    when(testStorage.getStoragePath()).thenReturn(testStorageDir);

    when(testStorage.getName()).thenReturn("localPaginatedClusterWithWALTestTwo");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File storageDirTwoFile = new File(testStorageDir);
    if (!storageDirTwoFile.exists())
      storageDirTwoFile.mkdirs();

    testDiskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 1648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, testStorage, null, false, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(testStorageDir);
    final OAtomicOperationsManager testAtomicOperationsManager = new OAtomicOperationsManager(null);

    when(testStorage.getStorageTransaction()).thenReturn(null);
    when(testStorage.getAtomicOperationsManager()).thenReturn(testAtomicOperationsManager);
    when(testStorage.getDiskCache()).thenReturn(testDiskCache);
    when(testStorage.getWALInstance()).thenReturn(null);
    when(testStorage.getVariableParser()).thenReturn(variableParser);
    when(testStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(testStorage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(testStorageDir);

    testCluster = new OPaginatedCluster();
    testCluster.configure(testStorage, 6, "testPaginatedClusterWithWALTest", buildDirectory, -1);
    testCluster.create(-1);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    writeAheadLog.delete();
    paginatedCluster.delete();
    diskCache.delete();

    testCluster.delete();
    testDiskCache.delete();

    File file = new File(storageDir);
    Assert.assertTrue(file.delete());

    file = new File(testStorageDir);
    Assert.assertTrue(file.delete());

    file = new File(buildDirectory);
    Assert.assertTrue(file.delete());
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

  @Override
  @Test(enabled = false)
  public void testCompressionNothing() throws Exception {
    super.testCompressionNothing();
  }

  @Override
  @Test(enabled = false)
  public void testCompressionSnappy() throws Exception {
    super.testCompressionSnappy();
  }

  @Override
  @Test(enabled = false)
  public void testRecordGrowFactor() throws Exception {
    super.testRecordGrowFactor();
  }

  @Override
  @Test(enabled = false)
  public void testRecordOverflowGrowFactor() throws Exception {
    super.testRecordOverflowGrowFactor();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    paginatedCluster.close();
    writeAheadLog.close();

    diskCache.clear();

    restoreClusterFromWAL();

    testCluster.close();

    assertClusterContentIsTheSame(testCluster.getName(), paginatedCluster.getName());

    testCluster.open();
    paginatedCluster.open();
  }

  private void restoreClusterFromWAL() throws IOException {
    OWriteAheadLog log = new OWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, 100L * 1024 * 1024 * 1024, storage);
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

          final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

          final long fileId = updatePageRecord.getFileId();
          final long pageIndex = updatePageRecord.getPageIndex();

          if (!testDiskCache.isOpen(fileId))
            testDiskCache.openFile(fileId);

          final OCacheEntry cacheEntry = testDiskCache.load(fileId, pageIndex, true);
          final OCachePointer cachePointer = cacheEntry.getCachePointer();
          cachePointer.acquireExclusiveLock();
          try {
            ODurablePage durablePage = new ODurablePage(cachePointer.getDataPointer(), ODurablePage.TrackMode.NONE);
            durablePage.restoreChanges(updatePageRecord.getChanges());
            durablePage.setLsn(updatePageRecord.getLsn());

            cacheEntry.markDirty();
          } finally {
            cachePointer.releaseExclusiveLock();
            testDiskCache.release(cacheEntry);
          }
        }
        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OUpdatePageRecord);
      }

      lsn = log.next(lsn);
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();
  }

  private void assertClusterContentIsTheSame(String expectedCluster, String actualCluster) throws IOException {
    File expectedDataFile = new File(testStorageDir, expectedCluster + ".pcl");
    RandomAccessFile datFileOne = new RandomAccessFile(expectedDataFile, "r");
    RandomAccessFile datFileTwo = new RandomAccessFile(new File(storageDir, actualCluster + ".pcl"), "r");

    assertFileContentIsTheSame(datFileOne, datFileTwo);

    datFileOne.close();
    datFileTwo.close();

    File expectedRIDMapFile = new File(testStorageDir, expectedCluster + ".cpm");
    RandomAccessFile ridMapOne = new RandomAccessFile(expectedRIDMapFile, "r");
    RandomAccessFile ridMapTwo = new RandomAccessFile(new File(storageDir, actualCluster + ".cpm"), "r");

    assertFileContentIsTheSame(ridMapOne, ridMapTwo);

    ridMapOne.close();
    ridMapTwo.close();

  }

  private void assertFileContentIsTheSame(RandomAccessFile datFileOne, RandomAccessFile datFileTwo) throws IOException {
    Assert.assertEquals(datFileOne.length(), datFileTwo.length());

    byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
    byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];

    datFileOne.seek(OAbstractFile.HEADER_SIZE);
    datFileTwo.seek(OAbstractFile.HEADER_SIZE);

    int bytesRead = datFileOne.read(expectedContent);
    while (bytesRead >= 0) {
      datFileTwo.readFully(actualContent, 0, bytesRead);

      Assert.assertEquals(expectedContent, actualContent);

      expectedContent = new byte[OClusterPage.PAGE_SIZE];
      actualContent = new byte[OClusterPage.PAGE_SIZE];
      bytesRead = datFileOne.read(expectedContent);
    }
  }
}
