package com.orientechnologies.orient.core.index.sbtreebonsai.local;

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
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.local.O2QCache;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;

/**
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public class OSBTreeBonsaiWAL extends OSBTreeBonsaiLocalTest {
  private String                                     buildDirectory;

  private String                                     actualStorageDir;
  private String                                     expectedStorageDir;

  private ODiskWriteAheadLog                         writeAheadLog;

  private O2QCache                                   actualReadCache;
  private OWriteCache                                actualWriteCache;

  private O2QCache                                   expectedReadCache;
  private OWriteCache                                expectedWriteCache;

  private OLocalPaginatedStorage                     actualStorage;

  private OSBTreeBonsaiLocal<Integer, OIdentifiable> expectedSBTree;

  private OAtomicOperationsManager                   actualAtomicOperationsManager;

  @BeforeClass
  @Override
  public void beforeClass() {
  }

  @AfterClass
  @Override
  public void afterClass() {
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/sbtreeWithWALTest";

    createExpectedSBTree();
    createActualSBTree();
  }

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {
    Assert.assertNull(actualAtomicOperationsManager.getCurrentOperation());

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
    actualStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    storageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    actualStorageDir = buildDirectory + "/sbtreeWithWALTestActual";
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeWithWALTesActual");
    when(actualStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(storageConfiguration));

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File actualStorageDirFile = new File(actualStorageDir);
    if (!actualStorageDirFile.exists())
      actualStorageDirFile.mkdirs();

    writeAheadLog = new ODiskWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, actualStorage);

    actualWriteCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000,
        writeAheadLog, 100, 1648L * 1024 * 1024, 1648L * 1024 * 1024 + 400L * 1024 * 1024 * 1024, actualStorage, false, 1);

    actualReadCache = new O2QCache(400L * 1024 * 1024 * 1024, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024,
        false);

    when(actualStorage.getStorageTransaction()).thenReturn(null);
    when(actualStorage.getAtomicOperationsManager()).thenReturn(actualAtomicOperationsManager);
    when(actualStorage.getReadCache()).thenReturn(actualReadCache);
    when(actualStorage.getWALInstance()).thenReturn(writeAheadLog);
    when(actualStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(actualStorage.getMode()).thenReturn("rw");

    actualAtomicOperationsManager = new OAtomicOperationsManager(actualStorage);

    when(storageConfiguration.getDirectory()).thenReturn(actualStorageDir);

    sbTree = new OSBTreeBonsaiLocal<Integer, OIdentifiable>("actualSBTree", ".sbt", true, actualStorage);
    sbTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE);
  }

  private void createExpectedSBTree() {
    final OLocalPaginatedStorage expectedStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    storageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    expectedStorageDir = buildDirectory + "/sbtreeWithWALTestExpected";
    when(expectedStorage.getStoragePath()).thenReturn(expectedStorageDir);
    when(expectedStorage.getName()).thenReturn("sbtreeWithWALTesExpected");
    when(expectedStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(storageConfiguration));

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File expectedStorageDirFile = new File(expectedStorageDir);
    if (!expectedStorageDirFile.exists())
      expectedStorageDirFile.mkdirs();

    expectedWriteCache = new OWOWCache(false, OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000,
        writeAheadLog, 100, 1648L * 1024 * 1024, 1648L * 1024 * 1024 + 400L * 1024 * 1024 * 1024, expectedStorage, false, 2);
    expectedReadCache = new O2QCache(400L * 1024 * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, false);

    OStorageVariableParser variableParser = new OStorageVariableParser(expectedStorageDir);
    OAtomicOperationsManager atomicOperationsManager = new OAtomicOperationsManager(null);

    when(expectedStorage.getStorageTransaction()).thenReturn(null);
    when(expectedStorage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);
    when(expectedStorage.getReadCache()).thenReturn(expectedReadCache);
    when(expectedStorage.getWALInstance()).thenReturn(null);
    when(expectedStorage.getVariableParser()).thenReturn(variableParser);
    when(expectedStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(expectedStorage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(expectedStorageDir);

    expectedSBTree = new OSBTreeBonsaiLocal<Integer, OIdentifiable>("expectedSBTree", ".sbt", true, expectedStorage);
    expectedSBTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE);
  }

  @Override
  public void testKeyPut() throws Exception {
    super.testKeyPut();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomUniform() throws Exception {
    super.testKeyPutRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomGaussian() throws Exception {
    super.testKeyPutRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomUniform() throws Exception {
    super.testKeyDeleteRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomGaussian() throws Exception {
    super.testKeyDeleteRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDelete() throws Exception {
    super.testKeyDelete();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyAddDelete() throws Exception {
    super.testKeyAddDelete();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeFirstEmpty();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeLastEmpty();

    assertFileRestoreFromWAL();
  }

  @Override
  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
    super.testAddKeyValuesAndRemoveFirstMiddleAndLastPages();

    assertFileRestoreFromWAL();
  }

  @Test(enabled = false)
  @Override
  public void testValuesMajor() {
    super.testValuesMajor();
  }

  @Test(enabled = false)
  @Override
  public void testValuesMinor() {
    super.testValuesMinor();
  }

  @Test(enabled = false)
  @Override
  public void testValuesBetween() {
    super.testValuesBetween();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    sbTree.close();
    writeAheadLog.close();
    expectedSBTree.close();

    actualReadCache.clear();

    restoreDataFromWAL();

    expectedReadCache.clear();

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
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord)
            continue;

          final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

          final long fileId = updatePageRecord.getFileId();
          final long pageIndex = updatePageRecord.getPageIndex();

          if (!expectedWriteCache.isOpen(fileId))
            expectedReadCache.openFile(fileId, expectedWriteCache);

          OCacheEntry cacheEntry = expectedReadCache.load(fileId, pageIndex, true, expectedWriteCache);
          if (cacheEntry == null) {
            do {
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
