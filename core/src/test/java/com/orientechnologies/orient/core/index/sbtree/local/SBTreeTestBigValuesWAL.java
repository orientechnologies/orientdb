package com.orientechnologies.orient.core.index.sbtree.local;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.local.O2QCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/2/13
 */
@Test
public class SBTreeTestBigValuesWAL extends SBTreeTestBigValues {
  static {
    OGlobalConfiguration.INDEX_TX_MODE.setValue("FULL");
    OGlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private String                   buildDirectory;

  private String                   actualStorageDir;
  private String                   expectedStorageDir;

  private ODiskWriteAheadLog       writeAheadLog;

  private OReadCache               actualReadCache;
  private OWriteCache              actualWriteCache;

  private OReadCache               expectedReadCache;
  private OWriteCache              expectedWriteCache;

  private OLocalPaginatedStorage   actualStorage;

  private OSBTree<Integer, byte[]> expectedSBTree;
  private OLocalPaginatedStorage   expectedStorage;
  private OStorageConfiguration    expectedStorageConfiguration;
  private OStorageConfiguration    actualStorageConfiguration;

  private OAtomicOperationsManager actualAtomicOperationsManager;

  @BeforeClass
  @Override
  public void beforeClass() {
    actualStorage = mock(OLocalPaginatedStorage.class);
    actualStorageConfiguration = mock(OStorageConfiguration.class);
    expectedStorage = mock(OLocalPaginatedStorage.class);
    expectedStorageConfiguration = mock(OStorageConfiguration.class);
  }

  @AfterClass
  @Override
  public void afterClass() {
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    Mockito.reset(actualStorage, expectedStorage, expectedStorageConfiguration, actualStorageConfiguration);

    buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/sbtreeWithBigValuesWALTest";

    createExpectedSBTree();
    createActualSBTree();
  }

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {
    sbTree.delete();
    expectedSBTree.delete();

    actualReadCache.deleteStorage(actualWriteCache);
    expectedReadCache.deleteStorage(expectedWriteCache);

    writeAheadLog.delete();

    Assert.assertTrue(new File(actualStorageDir).delete());
    Assert.assertTrue(new File(expectedStorageDir).delete());
    Assert.assertTrue(new File(buildDirectory).delete());

  }

  private void createActualSBTree() throws IOException {
    actualStorageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    actualStorageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    actualStorageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    actualStorageDir = buildDirectory + "/sbtreeWithBigValuesWALTestActual";
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeBigValuesWithWALTesActual");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File actualStorageDirFile = new File(actualStorageDir);
    if (!actualStorageDirFile.exists())
      actualStorageDirFile.mkdirs();

    OStorageVariableParser variableParser = new OStorageVariableParser(actualStorageDir);
    when(actualStorage.getVariableParser()).thenReturn(variableParser);
    when(actualStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(actualStorageConfiguration));

    writeAheadLog = new ODiskWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, actualStorage);

    when(actualStorage.getWALInstance()).thenReturn(writeAheadLog);
    actualAtomicOperationsManager = new OAtomicOperationsManager(actualStorage);

    actualWriteCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 100000,
        writeAheadLog, 100, 1648L * 1024 * 1024, 400L * 1024 * 1024 * 1024 + 1648L * 1024 * 1024, actualStorage, true, 1);
    actualReadCache = new O2QCache(400L * 1024 * 1024 * 1024, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024,
        true);

    when(actualStorage.getStorageTransaction()).thenReturn(null);
    when(actualStorage.getReadCache()).thenReturn(actualReadCache);
    when(actualStorage.getWALInstance()).thenReturn(writeAheadLog);
    when(actualStorage.getConfiguration()).thenReturn(actualStorageConfiguration);
    when(actualStorage.getAtomicOperationsManager()).thenReturn(actualAtomicOperationsManager);
    when(actualStorage.getMode()).thenReturn("rw");

    when(actualStorageConfiguration.getDirectory()).thenReturn(actualStorageDir);

    sbTree = new OSBTree<Integer, byte[]>("actualSBTree", ".sbt", true, ".nbt", actualStorage);
    sbTree.create(OIntegerSerializer.INSTANCE, OBinaryTypeSerializer.INSTANCE, null, 1, false);
  }

  private void createExpectedSBTree() {
    expectedStorageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    expectedStorageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    expectedStorageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    expectedStorageDir = buildDirectory + "/sbtreeWithBigValuesWALTestExpected";
    when(expectedStorage.getStoragePath()).thenReturn(expectedStorageDir);
    when(expectedStorage.getName()).thenReturn("sbtreeWithBigValuesWALTesExpected");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File expectedStorageDirFile = new File(expectedStorageDir);
    if (!expectedStorageDirFile.exists())
      expectedStorageDirFile.mkdirs();

    OStorageVariableParser variableParser = new OStorageVariableParser(expectedStorageDir);
    when(expectedStorage.getVariableParser()).thenReturn(variableParser);
    when(expectedStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(expectedStorageConfiguration));

    OAtomicOperationsManager atomicOperationsManager = new OAtomicOperationsManager(expectedStorage);

    expectedWriteCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000,
        writeAheadLog, 100, 1648L * 1024 * 1024, 1648L * 1024 * 1024 + 400L * 1024 * 1024 * 1024, expectedStorage, true, 1);
    expectedReadCache = new O2QCache(400L * 1024 * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, false);

    when(expectedStorage.getStorageTransaction()).thenReturn(null);
    when(expectedStorage.getReadCache()).thenReturn(expectedReadCache);
    when(expectedStorage.getWALInstance()).thenReturn(null);
    when(expectedStorage.getConfiguration()).thenReturn(expectedStorageConfiguration);
    when(expectedStorage.getMode()).thenReturn("rw");
    when(expectedStorage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);

    when(expectedStorageConfiguration.getDirectory()).thenReturn(expectedStorageDir);

    expectedSBTree = new OSBTree<Integer, byte[]>("expectedSBTree", ".sbt", true, ".nbt", expectedStorage);
    expectedSBTree.create(OIntegerSerializer.INSTANCE, OBinaryTypeSerializer.INSTANCE, null, 1, false);
  }

  @Override
  @Test(enabled = false)
  protected void doReset() {
    Mockito.reset(actualStorage, actualStorageConfiguration);

    when(actualStorage.getStorageTransaction()).thenReturn(null);
    when(actualStorage.getReadCache()).thenReturn(actualReadCache);
    when(actualStorage.getAtomicOperationsManager()).thenReturn(actualAtomicOperationsManager);
    when(actualStorage.getConfiguration()).thenReturn(actualStorageConfiguration);
    when(actualStorage.getMode()).thenReturn("rw");
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeBigValuesWithWALTesActual");
  }

  @Override
  public void testPut() throws Exception {
    logTestStart("testPut");
    super.testPut();
    logTestEnd("testPut");

    logStartDataRestore("testPut");
    assertFileRestoreFromWAL();
    logEndDataRestore("testPut");
  }

  @Override
  public void testKeyPutRandomUniform() throws Exception {
    logTestStart("testKeyPutRandomUniform");
    super.testKeyPutRandomUniform();
    logTestEnd("testKeyPutRandomUniform");

    logStartDataRestore("testKeyPutRandomUniform");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeyPutRandomUniform");
  }

  @Override
  public void testKeyPutRandomGaussian() throws Exception {
    logTestStart("testKeyPutRandomGaussian");
    super.testKeyPutRandomGaussian();
    logTestEnd("testKeyPutRandomGaussian");

    logStartDataRestore("testKeyPutRandomGaussian");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeyPutRandomGaussian");
  }

  @Override
  public void testKeyDeleteRandomUniform() throws Exception {
    logTestStart("testKeyDeleteRandomUniform");
    super.testKeyDeleteRandomUniform();
    logTestEnd("testKeyDeleteRandomUniform");

    logStartDataRestore("testKeyDeleteRandomUniform");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeyDeleteRandomUniform");
  }

  @Override
  public void testKeyDeleteRandomGaussian() throws Exception {
    logTestStart("testKeyDeleteRandomGaussian");
    super.testKeyDeleteRandomGaussian();
    logTestEnd("testKeyDeleteRandomGaussian");

    logStartDataRestore("testKeyDeleteRandomGaussian");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeyDeleteRandomGaussian");
  }

  @Override
  public void testKeyDelete() throws Exception {
    logTestStart("testKeyDelete");
    super.testKeyDelete();
    logTestEnd("testKeyDelete");

    logStartDataRestore("testKeyDelete");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeyDelete");
  }

  @Override
  public void testKeyAddDelete() throws Exception {
    logTestStart("testKeyAddDelete");
    super.testKeyAddDelete();
    logTestEnd("testKeyAddDelete");

    logStartDataRestore("testKeyAddDelete");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeyAddDelete");

  }

  @Override
  public void testKeysUpdateFromSmallToBig() throws Exception {
    logTestStart("testKeysUpdateFromSmallToBig");
    super.testKeysUpdateFromSmallToBig();
    logTestEnd("testKeysUpdateFromSmallToBig");

    logStartDataRestore("testKeysUpdateFromSmallToBig");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeysUpdateFromSmallToBig");
  }

  @Override
  public void testKeysUpdateFromBigToSmall() throws Exception {
    logTestStart("testKeysUpdateFromBigToSmall");
    super.testKeysUpdateFromBigToSmall();
    logTestEnd("testKeysUpdateFromBigToSmall");

    logStartDataRestore("testKeysUpdateFromBigToSmall");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeysUpdateFromBigToSmall");
  }

  @Override
  public void testKeysUpdateFromSmallToSmall() throws Exception {
    logTestStart("testKeysUpdateFromSmallToSmall");
    super.testKeysUpdateFromSmallToSmall();
    logTestEnd("testKeysUpdateFromSmallToSmall");

    logStartDataRestore("testKeysUpdateFromSmallToSmall");
    assertFileRestoreFromWAL();
    logEndDataRestore("testKeysUpdateFromSmallToSmall");
  }

  private void logEndDataRestore(String testName) {
    System.out.println(testName + ": end data restore.");
  }

  private void logStartDataRestore(String testName) {
    System.out.println(testName + ": start data restore.");
  }

  private void logTestEnd(String testName) {
    System.out.println(testName + ": end test.");
  }

  private void logTestStart(String testName) {
    System.out.println(testName + ": start test.");
  }

  private void assertFileRestoreFromWAL() throws IOException {
    sbTree.close();
    writeAheadLog.close();
    expectedSBTree.close();

    ((O2QCache) actualReadCache).clear();

    restoreDataFromWAL();

    ((O2QCache) expectedReadCache).clear();

    assertFileContentIsTheSame(expectedSBTree.getName(), sbTree.getName());
  }

  private void restoreDataFromWAL() throws IOException {
    ODiskWriteAheadLog log = new ODiskWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, actualStorage);
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
              || restoreRecord instanceof ONonTxOperationPerformedWALRecord || restoreRecord instanceof OFileCreatedWALRecord)
            continue;

          final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

          final long fileId = updatePageRecord.getFileId();
          final long pageIndex = updatePageRecord.getPageIndex();

          if (!expectedWriteCache.isOpen(fileId))
            expectedReadCache.openFile(fileId, expectedWriteCache);

          OCacheEntry cacheEntry = expectedReadCache.load(fileId, pageIndex, true, expectedWriteCache);
          if (cacheEntry == null) {
            do {
              if (cacheEntry != null)
                expectedReadCache.release(cacheEntry, expectedWriteCache);

              cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache);
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
            expectedReadCache.release(cacheEntry, expectedWriteCache);
          }
        }
        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof ONonTxOperationPerformedWALRecord
            || walRecord instanceof OFileCreatedWALRecord);
      }

      lsn = log.next(lsn);
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();
  }

  private void assertFileContentIsTheSame(String expectedBTree, String actualBTree) throws IOException {
    File expectedFile = new File(expectedStorageDir, expectedBTree + ".sbt");
    RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r");
    RandomAccessFile fileTwo = new RandomAccessFile(new File(actualStorageDir, actualBTree + ".sbt"), "r");

    Assert.assertEquals(fileOne.length(), fileTwo.length());

    byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
    byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];

    fileOne.seek(OFileClassic.HEADER_SIZE);
    fileTwo.seek(OFileClassic.HEADER_SIZE);

    int bytesRead = fileOne.read(expectedContent);
    while (bytesRead >= 0) {
      fileTwo.readFully(actualContent, 0, bytesRead);

      Assert.assertEquals(expectedContent, actualContent);

      expectedContent = new byte[OClusterPage.PAGE_SIZE];
      actualContent = new byte[OClusterPage.PAGE_SIZE];
      bytesRead = fileOne.read(expectedContent);
    }

    fileOne.close();
    fileTwo.close();
  }
}
