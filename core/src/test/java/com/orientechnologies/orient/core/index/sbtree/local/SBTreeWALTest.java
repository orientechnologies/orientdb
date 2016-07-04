package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.assertj.core.api.Assertions;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin
 * @since 8/27/13
 */
public class SBTreeWALTest extends SBTreeTest {
  static {
    OGlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private String buildDirectory;

  private ODatabaseDocumentTx expectedDatabaseDocumentTx;

  private String actualStorageDir;
  private String expectedStorageDir;

  private ODiskWriteAheadLog writeAheadLog;

  private OLocalPaginatedStorage actualStorage;
  private OReadCache             actualReadCache;
  private OWriteCache            actualWriteCache;

  private OLocalPaginatedStorage expectedStorage;
  private OReadCache             expectedReadCache;
  private OWriteCache            expectedWriteCache;

  @Before
  public void before() throws IOException {
    buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/sbtreeWithWALTest";

    final File buildDir = new File(buildDirectory);
    if (!buildDir.exists())
      buildDir.mkdirs();

    createExpectedSBTree();
    createActualSBTree();
  }

  @After
  @Override
  public void afterMethod() throws Exception {
    databaseDocumentTx.open("admin", "admin");
    databaseDocumentTx.drop();

    expectedDatabaseDocumentTx.open("admin", "admin");
    expectedDatabaseDocumentTx.drop();

    final File actualStorage = new File(actualStorageDir);
    if (actualStorage.exists())
      Assert.assertTrue(actualStorage.delete());

    final File expectedStorage = new File(expectedStorageDir);
    if (expectedStorage.exists())
      Assert.assertTrue(expectedStorage.delete());

    final File buildDir = new File(buildDirectory);
    if (buildDir.exists())
      Assert.assertTrue(buildDir.delete());
  }

  private void createActualSBTree() throws IOException {
    actualStorageDir = buildDirectory + "/sbtreeWithWALTestActual";

    File actualStorageDirFile = new File(actualStorageDir);
    if (!actualStorageDirFile.exists())
      actualStorageDirFile.mkdirs();

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + actualStorageDir);
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    } else {
      databaseDocumentTx.create();
    }

    actualStorage = (OLocalPaginatedStorage) databaseDocumentTx.getStorage();
    writeAheadLog = (ODiskWriteAheadLog) actualStorage.getWALInstance();
    writeAheadLog.preventCutTill(writeAheadLog.getFlushedLsn());

    actualReadCache = ((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getReadCache();

    sbTree = new OSBTree<Integer, OIdentifiable>("actualSBTree", ".sbt", true, ".nbt", actualStorage);
    sbTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false);
  }

  private void createExpectedSBTree() {
    expectedStorageDir = buildDirectory + "/sbtreeWithWALTestExpected";

    File expectedStorageDirFile = new File(expectedStorageDir);
    if (!expectedStorageDirFile.exists())
      expectedStorageDirFile.mkdirs();

    expectedDatabaseDocumentTx = new ODatabaseDocumentTx("plocal:" + expectedStorageDir);
    if (expectedDatabaseDocumentTx.exists()) {
      expectedDatabaseDocumentTx.open("admin", "admin");
      expectedDatabaseDocumentTx.drop();
    } else {
      expectedDatabaseDocumentTx.create();
    }

    expectedStorage = (OLocalPaginatedStorage) expectedDatabaseDocumentTx.getStorage();
    expectedReadCache = expectedStorage.getReadCache();
    expectedWriteCache = expectedStorage.getWriteCache();
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

  @Test
  @Ignore
  @Override
  public void testNullKeysInSBTree() {
    super.testNullKeysInSBTree();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesMajor() {
    super.testIterateEntriesMajor();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesMinor() {
    super.testIterateEntriesMinor();
  }

  @Test
  @Ignore
  @Override
  public void testIterateEntriesBetween() {
    super.testIterateEntriesBetween();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    OStorage storage = databaseDocumentTx.getStorage();
    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.close();
    storage.close(true, false);

    restoreDataFromWAL();

    expectedDatabaseDocumentTx.activateOnCurrentThread();
    expectedDatabaseDocumentTx.close();
    storage = expectedDatabaseDocumentTx.getStorage();
    storage.close(true, false);

    assertFileContentIsTheSame("expectedSBTree", sbTree.getName());
  }

  private void restoreDataFromWAL() throws IOException {
    ODiskWriteAheadLog log = new ODiskWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, null, actualStorage);
    OLogSequenceNumber lsn = log.begin();

    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();

    boolean atomicChangeIsProcessed = false;
    while (lsn != null) {
      OWALRecord walRecord = log.read(lsn);
      if (walRecord instanceof OOperationUnitBodyRecord)
        atomicUnit.add(walRecord);

      if (!atomicChangeIsProcessed) {
        if (walRecord instanceof OAtomicUnitStartRecord)
          atomicChangeIsProcessed = true;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        atomicChangeIsProcessed = false;

        for (OWALRecord restoreRecord : atomicUnit) {
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord
              || restoreRecord instanceof ONonTxOperationPerformedWALRecord)
            continue;

          if (restoreRecord instanceof OFileCreatedWALRecord) {
            final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord) restoreRecord;
            final String fileName = fileCreatedCreatedRecord.getFileName().replace("actualSBTree", "expectedSBTree");

            if (!expectedWriteCache.exists(fileName))
              expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
          } else {
            final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

            final long fileId = updatePageRecord.getFileId();
            final long pageIndex = updatePageRecord.getPageIndex();

            OCacheEntry cacheEntry = expectedReadCache.load(fileId, pageIndex, true, expectedWriteCache, 1);
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

      //      Assert.assertEquals(expectedContent, actualContent);

      Assertions.assertThat(expectedContent).isEqualTo(actualContent);
      expectedContent = new byte[OClusterPage.PAGE_SIZE];
      actualContent = new byte[OClusterPage.PAGE_SIZE];
      bytesRead = fileOne.read(expectedContent);
    }

    fileOne.close();
    fileTwo.close();
  }
}
