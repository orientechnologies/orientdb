package com.orientechnologies.orient.core.index.sbtree.local;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
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
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 10/2/13
 */
@Test
public class SBTreeTestBigValuesWAL extends SBTreeTestBigValues {
  static {
    OGlobalConfiguration.INDEX_TX_MODE.setValue("FULL");
  }

  private String                   buildDirectory;

  private String                   actualStorageDir;
  private String                   expectedStorageDir;

  private OWriteAheadLog           writeAheadLog;

  private ODiskCache               actualDiskCache;
  private ODiskCache               expectedDiskCache;

  private OLocalPaginatedStorage   actualStorage;

  private OSBTree<Integer, byte[]> expectedSBTree;
  private OLocalPaginatedStorage   expectedStorage;
  private OStorageConfiguration    expectedStorageConfiguration;
  private OStorageConfiguration    actualStorageConfiguration;

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

    actualDiskCache.delete();
    expectedDiskCache.delete();

    writeAheadLog.delete();

    Assert.assertTrue(new File(actualStorageDir).delete());
    Assert.assertTrue(new File(expectedStorageDir).delete());
    Assert.assertTrue(new File(buildDirectory).delete());

  }

  private void createActualSBTree() throws IOException {
    actualStorageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    actualStorageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    actualStorageDir = buildDirectory + "/sbtreeWithBigValuesWALTestActual";
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeBigValuesWithWALTesActual");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File actualStorageDirFile = new File(actualStorageDir);
    if (!actualStorageDirFile.exists())
      actualStorageDirFile.mkdirs();

    writeAheadLog = new OWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, 100L * 1024 * 1024 * 1024, actualStorage);

    actualDiskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 1648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, actualStorage, null, false, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(actualStorageDir);

    when(actualStorage.getStorageTransaction()).thenReturn(null);
    when(actualStorage.getDiskCache()).thenReturn(actualDiskCache);
    when(actualStorage.getWALInstance()).thenReturn(writeAheadLog);
    when(actualStorage.getVariableParser()).thenReturn(variableParser);
    when(actualStorage.getConfiguration()).thenReturn(actualStorageConfiguration);
    when(actualStorage.getMode()).thenReturn("rw");

    when(actualStorageConfiguration.getDirectory()).thenReturn(actualStorageDir);

    sbTree = new OSBTree<Integer, byte[]>(".sbt", 1, true);
    sbTree.create("actualSBTree", OIntegerSerializer.INSTANCE, OBinaryTypeSerializer.INSTANCE, null, actualStorage);
  }

  private void createExpectedSBTree() {
    expectedStorageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    expectedStorageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    expectedStorageDir = buildDirectory + "/sbtreeWithBigValuesWALTestExpected";
    when(expectedStorage.getStoragePath()).thenReturn(expectedStorageDir);
    when(expectedStorage.getName()).thenReturn("sbtreeWithBigValuesWALTesExpected");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File expectedStorageDirFile = new File(expectedStorageDir);
    if (!expectedStorageDirFile.exists())
      expectedStorageDirFile.mkdirs();

    expectedDiskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 1648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, expectedStorage, null, false, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(expectedStorageDir);

    when(expectedStorage.getStorageTransaction()).thenReturn(null);
    when(expectedStorage.getDiskCache()).thenReturn(expectedDiskCache);
    when(expectedStorage.getWALInstance()).thenReturn(null);
    when(expectedStorage.getVariableParser()).thenReturn(variableParser);
    when(expectedStorage.getConfiguration()).thenReturn(expectedStorageConfiguration);
    when(expectedStorage.getMode()).thenReturn("rw");

    when(expectedStorageConfiguration.getDirectory()).thenReturn(expectedStorageDir);

    expectedSBTree = new OSBTree<Integer, byte[]>(".sbt", 1, true);
    expectedSBTree.create("expectedSBTree", OIntegerSerializer.INSTANCE, OBinaryTypeSerializer.INSTANCE, null, expectedStorage);
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

    actualDiskCache.clear();

    restoreDataFromWAL();

    expectedDiskCache.clear();

    assertFileContentIsTheSame(expectedSBTree.getName(), sbTree.getName());
  }

  private void restoreDataFromWAL() throws IOException {
    OWriteAheadLog log = new OWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, 100L * 1024 * 1024 * 1024, actualStorage);
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

          if (!expectedDiskCache.isOpen(fileId))
            expectedDiskCache.openFile(fileId);

          final OCacheEntry cacheEntry = expectedDiskCache.load(fileId, pageIndex, true);
          final OCachePointer cachePointer = cacheEntry.getCachePointer();
          cachePointer.acquireExclusiveLock();
          try {
            ODurablePage durablePage = new ODurablePage(cachePointer.getDataPointer(), ODurablePage.TrackMode.NONE);
            durablePage.restoreChanges(updatePageRecord.getChanges());
            durablePage.setLsn(updatePageRecord.getLsn());

            cacheEntry.markDirty();
          } finally {
            cachePointer.releaseExclusiveLock();
            expectedDiskCache.release(cacheEntry);
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

  private void assertFileContentIsTheSame(String expectedBTree, String actualBTree) throws IOException {
    File expectedFile = new File(expectedStorageDir, expectedBTree + ".sbt");
    RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r");
    RandomAccessFile fileTwo = new RandomAccessFile(new File(actualStorageDir, actualBTree + ".sbt"), "r");

    Assert.assertEquals(fileOne.length(), fileTwo.length());

    byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
    byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];

    fileOne.seek(OAbstractFile.HEADER_SIZE);
    fileTwo.seek(OAbstractFile.HEADER_SIZE);

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
