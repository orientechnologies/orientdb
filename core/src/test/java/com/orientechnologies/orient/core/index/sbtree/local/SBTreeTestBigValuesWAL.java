package com.orientechnologies.orient.core.index.sbtree.local;

import com.orientechnologies.common.serialization.types.OBinaryTypeSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 10/2/13
 */
@Test
public class SBTreeTestBigValuesWAL extends SBTreeTestBigValues {
  static {
    OGlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private ODatabaseDocumentTx expectedDatabaseDocumentTx;

  private String buildDirectory;

  private String actualStorageDir;
  private String expectedStorageDir;

  private ODiskWriteAheadLog writeAheadLog;

  private OReadCache actualReadCache;

  private OReadCache expectedReadCache;

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

    buildDirectory += "/sbtreeWithBigValuesWALTest";

    actualStorageDir = buildDirectory + File.separator + this.getClass().getSimpleName() + "Actual";

    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + actualStorageDir);
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    final String expectedStorageName = this.getClass().getSimpleName() + "Expected";
    expectedStorageDir = buildDirectory + File.separator + expectedStorageName;

    expectedDatabaseDocumentTx = new ODatabaseDocumentTx("plocal:" + expectedStorageDir);
    if (expectedDatabaseDocumentTx.exists()) {
      expectedDatabaseDocumentTx.open("admin", "admin");
      expectedDatabaseDocumentTx.drop();
    }

    expectedDatabaseDocumentTx.create();

    expectedReadCache = ((OLocalPaginatedStorage) expectedDatabaseDocumentTx.getStorage()).getReadCache();

    createActualSBTree();
  }

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {
    if (databaseDocumentTx.isClosed())
      databaseDocumentTx.open("admin", "admin");

    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.drop();

    if (expectedDatabaseDocumentTx.isClosed())
      expectedDatabaseDocumentTx.open("admin", "admin");

    expectedDatabaseDocumentTx.activateOnCurrentThread();
    expectedDatabaseDocumentTx.drop();

    Assert.assertTrue(new File(actualStorageDir).delete());
    Assert.assertTrue(new File(expectedStorageDir).delete());
    Assert.assertTrue(new File(buildDirectory).delete());

  }

  private void createActualSBTree() throws IOException {
    writeAheadLog = (ODiskWriteAheadLog) ((OLocalPaginatedStorage) databaseDocumentTx.getStorage()).getWALInstance();
    writeAheadLog.preventCutTill(writeAheadLog.getFlushedLsn());

    actualReadCache = ((OLocalPaginatedStorage) databaseDocumentTx.getStorage()).getReadCache();

    sbTree = new OSBTree<Integer, byte[]>("actualSBTree", ".sbt", true, ".nbt",
        (OLocalPaginatedStorage) databaseDocumentTx.getStorage());
    sbTree.create(OIntegerSerializer.INSTANCE, OBinaryTypeSerializer.INSTANCE, null, 1, false);
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
    actualReadCache.clear();

    restoreDataFromWAL();

    expectedReadCache.clear();

    assertFileContentIsTheSame("expectedSBTree", sbTree.getName());
  }

  private void restoreDataFromWAL() throws IOException {
    OWriteAheadLog log = ((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getWALInstance();

    OLogSequenceNumber lsn = log.begin();

    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();
    List<OWALRecord> batch = new ArrayList<OWALRecord>();

    boolean atomicChangeIsProcessed = false;
    while (lsn != null) {
      OWALRecord walRecord = log.read(lsn);
      batch.add(walRecord);

      if (batch.size() >= 1000) {
        atomicChangeIsProcessed = restoreDataFromBatch(atomicChangeIsProcessed, atomicUnit, batch);
        batch = new ArrayList<OWALRecord>();
      }

      lsn = log.next(lsn);
    }

    if (batch.size() > 0) {
      restoreDataFromBatch(atomicChangeIsProcessed, atomicUnit, batch);
      batch = null;
    }

    Assert.assertTrue(atomicUnit.isEmpty());

    OWriteCache writeCache = ((OAbstractPaginatedStorage) expectedDatabaseDocumentTx.getStorage()).getWriteCache();
    writeCache.flush();
  }

  private boolean restoreDataFromBatch(boolean atomicChangeIsProcessed, List<OWALRecord> atomicUnit, List<OWALRecord> records)
      throws IOException {

    final OReadCache expectedReadCache = ((OAbstractPaginatedStorage) expectedDatabaseDocumentTx.getStorage()).getReadCache();
    final OWriteCache expectedWriteCache = ((OAbstractPaginatedStorage) expectedDatabaseDocumentTx.getStorage()).getWriteCache();

    for (OWALRecord walRecord : records) {
      atomicUnit.add(walRecord);

      if (!atomicChangeIsProcessed && walRecord instanceof OAtomicUnitStartRecord) {
        atomicChangeIsProcessed = true;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        atomicChangeIsProcessed = false;

        for (OWALRecord restoreRecord : atomicUnit) {
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord
              || restoreRecord instanceof ONonTxOperationPerformedWALRecord || restoreRecord instanceof OFullCheckpointStartRecord
              || restoreRecord instanceof OCheckpointEndRecord || restoreRecord instanceof OFuzzyCheckpointStartRecord ||
              restoreRecord instanceof OFuzzyCheckpointEndRecord)
            continue;

          if (restoreRecord instanceof OUpdatePageRecord) {
            final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

            final long fileId = updatePageRecord.getFileId();
            final long pageIndex = updatePageRecord.getPageIndex();

            if (!expectedWriteCache.isOpen(fileId))
              expectedReadCache.openFile(fileId, expectedWriteCache);

            OCacheEntry cacheEntry = expectedReadCache.load(fileId, pageIndex, true, expectedWriteCache, 1);
            if (cacheEntry == null)
              do {
                cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache);
              } while (cacheEntry.getPageIndex() != pageIndex);

            cacheEntry.acquireExclusiveLock();
            try {
              ODurablePage durablePage = new ODurablePage(cacheEntry, null);
              durablePage.restoreChanges(updatePageRecord.getChanges());
              durablePage.setLsn(updatePageRecord.getLsn());
            } finally {
              cacheEntry.releaseExclusiveLock();
              expectedReadCache.release(cacheEntry, expectedWriteCache);
            }
          } else if (restoreRecord instanceof OFileCreatedWALRecord) {
            final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord) restoreRecord;
            String fileName = fileCreatedCreatedRecord.getFileName().replace("actualSBTree", "expectedSBTree");

            if (expectedWriteCache.exists(fileName))
              expectedReadCache.openFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
            else
              expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
          }
        }

        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof OFileCreatedWALRecord
            || walRecord instanceof ONonTxOperationPerformedWALRecord || walRecord instanceof OFullCheckpointStartRecord
            || walRecord instanceof OCheckpointEndRecord || walRecord instanceof OFuzzyCheckpointStartRecord ||
            walRecord instanceof OFuzzyCheckpointEndRecord);
      }

    }

    return atomicChangeIsProcessed;
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
