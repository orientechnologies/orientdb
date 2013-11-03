package com.orientechnologies.orient.core.index.sbtreebonsai.local;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageClusterConfiguration;
import com.orientechnologies.orient.core.config.OStorageConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OReadWriteDiskCache;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.fs.OAbstractFile;
import com.orientechnologies.orient.core.storage.impl.local.OStorageVariableParser;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;

/**
 * @author Andrey Lomakin
 * @since 8/27/13
 */
@Test(enabled = false)
public class OSBTreeBonsaiWAL extends OSBTreeBonsaiTest {
  private String                                buildDirectory;

  private String                                actualStorageDir;
  private String                                expectedStorageDir;

  private OWriteAheadLog                        writeAheadLog;

  private ODiskCache                            actualDiskCache;
  private ODiskCache                            expectedDiskCache;

  private OLocalPaginatedStorage                actualStorage;

  private OSBTreeBonsai<Integer, OIdentifiable> expectedSBTree;

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
    actualStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    actualStorageDir = buildDirectory + "/sbtreeWithWALTestActual";
    when(actualStorage.getStoragePath()).thenReturn(actualStorageDir);
    when(actualStorage.getName()).thenReturn("sbtreeWithWALTesActual");

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
    when(actualStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(actualStorage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(actualStorageDir);

    sbTree = new OSBTreeBonsai<Integer, OIdentifiable>(".sbt", 1, false);
    sbTree.create("actualSBTree", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, actualStorage);
  }

  private void createExpectedSBTree() {
    final OLocalPaginatedStorage expectedStorage = mock(OLocalPaginatedStorage.class);
    OStorageConfiguration storageConfiguration = mock(OStorageConfiguration.class);
    storageConfiguration.clusters = new ArrayList<OStorageClusterConfiguration>();
    storageConfiguration.fileTemplate = new OStorageSegmentConfiguration();

    expectedStorageDir = buildDirectory + "/sbtreeWithWALTestExpected";
    when(expectedStorage.getStoragePath()).thenReturn(expectedStorageDir);
    when(expectedStorage.getName()).thenReturn("sbtreeWithWALTesExpected");

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
    when(expectedStorage.getConfiguration()).thenReturn(storageConfiguration);
    when(expectedStorage.getMode()).thenReturn("rw");

    when(storageConfiguration.getDirectory()).thenReturn(expectedStorageDir);

    expectedSBTree = new OSBTreeBonsai<Integer, OIdentifiable>(".sbt", 1, false);
    expectedSBTree.create("expectedSBTree", OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, expectedStorage);
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
