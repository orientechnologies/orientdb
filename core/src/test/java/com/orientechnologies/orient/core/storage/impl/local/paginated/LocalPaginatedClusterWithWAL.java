package com.orientechnologies.orient.core.storage.impl.local.paginated;

import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.storage.cache.local.O2QCache;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFileCreatedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ONonTxOperationPerformedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin
 * @since 5/8/13
 */
@Test
public class LocalPaginatedClusterWithWAL extends LocalPaginatedClusterTest {
  {
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(1000000000);
  }
  private ODiskWriteAheadLog     writeAheadLog;

  private OPaginatedCluster      testCluster;

  private OReadCache             testReadCache;
  private OWriteCache            testWriteCache;

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
    when(storageConfiguration.getContextConfiguration()).thenReturn(new OContextConfiguration());
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    storageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    storageDir = buildDirectory + "/localPaginatedClusterWithWALTestOne";
    when(storage.getStoragePath()).thenReturn(storageDir);
    when(storage.getName()).thenReturn("localPaginatedClusterWithWALTestOne");
    when(storage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(storageConfiguration));
    when(storage.getVariableParser()).thenReturn(new OStorageVariableParser(storageDir));

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File storageDirOneFile = new File(storageDir);
    if (!storageDirOneFile.exists())
      storageDirOneFile.mkdirs();

    writeAheadLog = new ODiskWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, storage);

    writeCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, writeAheadLog,
        100, 1648L * 1024 * 1024, 2 * 1648L * 1024 * 1024, storage, false, 1);

    readCache = new O2QCache(1648L * 1024 * 1024, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, false);

    when(storage.getReadCache()).thenReturn(readCache);
    when(storage.getWriteCache()).thenReturn(writeCache);
    when(storage.getStorageTransaction()).thenReturn(null);
    when(storage.getWALInstance()).thenReturn(writeAheadLog);
    atomicOperationsManager = new OAtomicOperationsManager(storage);
    when(storage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);
    when(storage.getConfiguration()).thenReturn(storageConfiguration);
    when(storage.getMode()).thenReturn("rw");
    when(storageConfiguration.getDirectory()).thenReturn(storageDir);

    paginatedCluster = new OPaginatedCluster("testPaginatedClusterWithWALTest", storage);
    paginatedCluster.configure(storage, 6, "testPaginatedClusterWithWALTest", buildDirectory, -1);
    paginatedCluster.create(-1);
  }

  private void createTestPaginatedCluster() throws IOException {
    testStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    storageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;
    when(storageConfiguration.getContextConfiguration()).thenReturn(new OContextConfiguration());

    testStorageDir = buildDirectory + "/localPaginatedClusterWithWALTestTwo";
    when(testStorage.getStoragePath()).thenReturn(testStorageDir);
    when(testStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(storageConfiguration));

    when(testStorage.getName()).thenReturn("localPaginatedClusterWithWALTestTwo");
    when(testStorage.getVariableParser()).thenReturn(new OStorageVariableParser(testStorageDir));

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File storageDirTwoFile = new File(testStorageDir);
    if (!storageDirTwoFile.exists())
      storageDirTwoFile.mkdirs();

    testWriteCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000,
        writeAheadLog, 100, 1648L * 1024 * 1024, 1648L * 1024 * 1024 + 400L * 1024 * 1024 * 1024, testStorage, false, 1);

    testReadCache = new O2QCache(400L * 1024 * 1024 * 1024, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024,
        false);

    OStorageVariableParser variableParser = new OStorageVariableParser(testStorageDir);
    final OAtomicOperationsManager testAtomicOperationsManager = new OAtomicOperationsManager(testStorage);

    when(testStorage.getReadCache()).thenReturn(testReadCache);
    when(testStorage.getWriteCache()).thenReturn(testWriteCache);
    when(testStorage.getWALInstance()).thenReturn(null);
    when(testStorage.getStorageTransaction()).thenReturn(null);
    when(testStorage.getAtomicOperationsManager()).thenReturn(testAtomicOperationsManager);

    when(testStorage.getVariableParser()).thenReturn(variableParser);
    when(testStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(testStorage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(testStorageDir);

    testCluster = new OPaginatedCluster("testPaginatedClusterWithWALTest", testStorage);
    testCluster.configure(testStorage, 6, "testPaginatedClusterWithWALTest", buildDirectory, -1);
    testCluster.create(-1);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    Assert.assertNull(atomicOperationsManager.getCurrentOperation());

    writeAheadLog.delete();
    readCache.deleteStorage(writeCache);

    testCluster.delete();
    testReadCache.deleteStorage(testWriteCache);

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
  public void testHideHalfSmallRecords() throws IOException {
    super.testHideHalfSmallRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testHideHalfBigRecords() throws IOException {
    super.testHideHalfBigRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testHideHalfRecords() throws IOException {
    super.testHideHalfRecords();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testHideHalfRecordsAndAddAnotherHalfAgain() throws IOException {
    super.testHideHalfRecordsAndAddAnotherHalfAgain();

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

    readCache.clear();

    restoreClusterFromWAL();

    testCluster.close();

    assertClusterContentIsTheSame(testCluster.getName(), paginatedCluster.getName());

    testCluster.open();
    paginatedCluster.open();
  }

  private void restoreClusterFromWAL() throws IOException {
    ODiskWriteAheadLog log = new ODiskWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, storage);
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
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord
              || restoreRecord instanceof OFileCreatedWALRecord || restoreRecord instanceof ONonTxOperationPerformedWALRecord)
            continue;

          final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

          final long fileId = updatePageRecord.getFileId();
          final long pageIndex = updatePageRecord.getPageIndex();

          if (!testWriteCache.isOpen(fileId))
            testReadCache.openFile(fileId, testWriteCache);

          OCacheEntry cacheEntry = testReadCache.load(fileId, pageIndex, true, testWriteCache);
          if (cacheEntry == null) {
            do {
              if (cacheEntry != null)
                readCache.release(cacheEntry, testWriteCache);

              cacheEntry = testReadCache.allocateNewPage(fileId, testWriteCache);
            } while (cacheEntry.getPageIndex() != pageIndex);
          }
          cacheEntry.acquireExclusiveLock();
          try {
            ODurablePage durablePage = new ODurablePage(cacheEntry, null);
            durablePage.restoreChanges(updatePageRecord.getChanges());
            durablePage.setLsn(updatePageRecord.getLsn());

            cacheEntry.markDirty();
          } finally {
            cacheEntry.releaseExclusiveLock();
            testReadCache.release(cacheEntry, testWriteCache);
          }
        }
        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof OFileCreatedWALRecord
            || walRecord instanceof ONonTxOperationPerformedWALRecord);
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

    datFileOne.seek(OFileClassic.HEADER_SIZE);
    datFileTwo.seek(OFileClassic.HEADER_SIZE);

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
