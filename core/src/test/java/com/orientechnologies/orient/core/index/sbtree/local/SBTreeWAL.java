package com.orientechnologies.orient.core.index.sbtree.local;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import com.orientechnologies.orient.core.db.record.OCurrentStorageComponentsFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import org.mockito.Mockito;
import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteDiskCache;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.fs.OAbstractFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin
 * @since 8/27/13
 */
@Test
public class SBTreeWAL extends SBTreeTest {
  static {
    OGlobalConfiguration.INDEX_TX_MODE.setValue("FULL");
    OGlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private String                          buildDirectory;

  private String                          actualStorageDir;
  private String                          expectedStorageDir;

  private ODiskWriteAheadLog              writeAheadLog;

  private ODiskCache                      actualDiskCache;
  private ODiskCache                      expectedDiskCache;

  private OLocalPaginatedStorage          actualStorage;

  private OSBTree<Integer, OIdentifiable> expectedSBTree;

  private OLocalPaginatedStorage          expectedStorage;
  private OStorageConfiguration           expectedStorageConfiguration;
  private OStorageConfiguration           actualStorageConfiguration;

  private OAtomicOperationsManager        actualAtomicOperationsManager;

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
    actualStorageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    actualStorageDir = buildDirectory + "/sbtreeWithWALTestActual";
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeWithWALTesActual");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File actualStorageDirFile = new File(actualStorageDir);
    if (!actualStorageDirFile.exists())
      actualStorageDirFile.mkdirs();

    writeAheadLog = new ODiskWriteAheadLog(6000, -1, 10 * 1024L * OWALPage.PAGE_SIZE, actualStorage);

    OStorageVariableParser variableParser = new OStorageVariableParser(actualStorageDir);
    when(actualStorage.getVariableParser()).thenReturn(variableParser);
    when(actualStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(actualStorageConfiguration));

    when(actualStorage.getWALInstance()).thenReturn(writeAheadLog);
    actualAtomicOperationsManager = new OAtomicOperationsManager(actualStorage);

    actualDiskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 1648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, actualStorage, null, false, false);

    when(actualStorage.getStorageTransaction()).thenReturn(null);
    when(actualStorage.getDiskCache()).thenReturn(actualDiskCache);
    when(actualStorage.getAtomicOperationsManager()).thenReturn(actualAtomicOperationsManager);
    when(actualStorage.getConfiguration()).thenReturn(actualStorageConfiguration);
    when(actualStorage.getMode()).thenReturn("rw");

    when(actualStorageConfiguration.getDirectory()).thenReturn(actualStorageDir);

    sbTree = new OSBTree<Integer, OIdentifiable>(".sbt", true, ".nbt", actualStorage);
    sbTree.create("actualSBTree", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false);
  }

  private void createExpectedSBTree() {
    expectedStorageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    expectedStorageConfiguration.fileTemplate = new OStorageSegmentConfiguration();
    expectedStorageConfiguration.binaryFormatVersion = Integer.MAX_VALUE;

    expectedStorageDir = buildDirectory + "/sbtreeWithWALTestExpected";
    when(expectedStorage.getStoragePath()).thenReturn(expectedStorageDir);
    when(expectedStorage.getName()).thenReturn("sbtreeWithWALTesExpected");

    File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    File expectedStorageDirFile = new File(expectedStorageDir);
    if (!expectedStorageDirFile.exists())
      expectedStorageDirFile.mkdirs();

    OStorageVariableParser variableParser = new OStorageVariableParser(expectedStorageDir);
    when(expectedStorage.getVariableParser()).thenReturn(variableParser);
    when(expectedStorage.getComponentsFactory()).thenReturn(new OCurrentStorageComponentsFactory(expectedStorageConfiguration));

    expectedDiskCache = new OReadWriteDiskCache(400L * 1024 * 1024 * 1024, 1648L * 1024 * 1024,
        OGlobalConfiguration.DISK_CACHE_PAGE_SIZE.getValueAsInteger() * 1024, 1000000, 100, expectedStorage, null, false, false);

    OAtomicOperationsManager atomicOperationsManager = new OAtomicOperationsManager(expectedStorage);

    when(expectedStorage.getStorageTransaction()).thenReturn(null);
    when(expectedStorage.getDiskCache()).thenReturn(expectedDiskCache);
    when(expectedStorage.getWALInstance()).thenReturn(null);
    when(expectedStorage.getAtomicOperationsManager()).thenReturn(atomicOperationsManager);

    when(expectedStorage.getConfiguration()).thenReturn(expectedStorageConfiguration);
    when(expectedStorage.getMode()).thenReturn("rw");

    when(expectedStorageConfiguration.getDirectory()).thenReturn(expectedStorageDir);

    expectedSBTree = new OSBTree<Integer, OIdentifiable>(".sbt", true, ".nbt", expectedStorage);
    expectedSBTree.create("expectedSBTree", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false);
  }

  @Override
  @Test(enabled = false)
  protected void doReset() {
    Mockito.reset(actualStorage, actualStorageConfiguration);

    when(actualStorage.getStorageTransaction()).thenReturn(null);
    when(actualStorage.getDiskCache()).thenReturn(actualDiskCache);
    when(actualStorage.getAtomicOperationsManager()).thenReturn(actualAtomicOperationsManager);
    when(actualStorage.getConfiguration()).thenReturn(actualStorageConfiguration);
    when(actualStorage.getMode()).thenReturn("rw");
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeWithWALTesActual");
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
  public void testNullKeysInSBTree() {
    super.testNullKeysInSBTree();
  }

  @Test(enabled = false)
  @Override
  public void testIterateEntriesMajor() {
    super.testIterateEntriesMajor();
  }

  @Test(enabled = false)
  @Override
  public void testIterateEntriesMinor() {
    super.testIterateEntriesMinor();
  }

  @Test(enabled = false)
  @Override
  public void testIterateEntriesBetween() {
    super.testIterateEntriesBetween();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    sbTree.close();
    writeAheadLog.close();
    expectedSBTree.close();

    ((OReadWriteDiskCache) actualDiskCache).clear();

    restoreDataFromWAL();

    ((OReadWriteDiskCache) expectedDiskCache).clear();

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

          if (!expectedDiskCache.isOpen(fileId))
            expectedDiskCache.openFile(fileId);

          OCacheEntry cacheEntry = expectedDiskCache.load(fileId, pageIndex, true);
          if (cacheEntry == null) {
            do {
              cacheEntry = expectedDiskCache.allocateNewPage(fileId);
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
            expectedDiskCache.release(cacheEntry);
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
