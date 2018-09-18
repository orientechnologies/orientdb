package com.orientechnologies.orient.core.storage.index.sbtree.local;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFileCreatedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointEndRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointStartRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ONonTxOperationPerformedWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitBodyRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OCASDiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import org.assertj.core.api.Assertions;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 8/27/13
 */
public class SBTreeWALTestIT extends SBTreeTestIT {
  static {
    OGlobalConfiguration.FILE_LOCK.setValue(false);
  }

  private OLocalPaginatedStorage actualStorage;
  private OWriteCache            actualWriteCache;

  private ODatabaseSession       expectedDatabaseDocumentTx;
  private OLocalPaginatedStorage expectedStorage;
  private OReadCache             expectedReadCache;
  private OWriteCache            expectedWriteCache;

  private String expectedStorageDir;
  private String actualStorageDir;

  private static final String DIR_NAME         = SBTreeWALTestIT.class.getSimpleName();
  private static final String ACTUAL_DB_NAME   = "sbtreeWithWALTestActual";
  private static final String EXPECTED_DB_NAME = "sbtreeWithWALTestExpected";

  @Before
  public void before() {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    buildDirectory += "/" + DIR_NAME;

    final File buildDir = new File(buildDirectory);
    OFileUtils.deleteRecursively(buildDir);

    orientDB = new OrientDB("plocal:" + buildDir, OrientDBConfig.defaultConfig());

    createExpectedSBTree();
    createActualSBTree();
  }

  @After
  @Override
  public void afterMethod() throws Exception {
    orientDB.drop(ACTUAL_DB_NAME);
    orientDB.drop(EXPECTED_DB_NAME);
    orientDB.close();
  }

  private void createActualSBTree() {
    orientDB.create(ACTUAL_DB_NAME, ODatabaseType.PLOCAL);

    databaseDocumentTx = orientDB.open(ACTUAL_DB_NAME, "admin", "admin");
    actualStorage = (OLocalPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage();
    actualStorageDir = actualStorage.getStoragePath().toString();
    OCASDiskWriteAheadLog writeAheadLog = (OCASDiskWriteAheadLog) actualStorage.getWALInstance();

    actualStorage.synch();
    writeAheadLog.addCutTillLimit(writeAheadLog.getFlushedLsn());

    actualWriteCache = actualStorage.getWriteCache();

    sbTree = new OSBTree<>("actualSBTree", ".sbt", ".nbt", actualStorage);
    sbTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE, null, 1, false, null);
  }

  private void createExpectedSBTree() {
    orientDB.create(EXPECTED_DB_NAME, ODatabaseType.PLOCAL);

    expectedDatabaseDocumentTx = orientDB.open(EXPECTED_DB_NAME, "admin", "admin");
    expectedStorage = (OLocalPaginatedStorage) ((ODatabaseInternal) expectedDatabaseDocumentTx).getStorage();
    expectedReadCache = expectedStorage.getReadCache();
    expectedWriteCache = expectedStorage.getWriteCache();

    expectedStorageDir = expectedStorage.getStoragePath().toString();
  }

  @Override
  @Test
  public void testKeyPut() throws Exception {
    super.testKeyPut();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyPutRandomUniform() throws Exception {
    super.testKeyPutRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyPutRandomGaussian() throws Exception {
    super.testKeyPutRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Override
  @Test
  public void testKeyDeleteRandomUniform() throws Exception {
    super.testKeyDeleteRandomUniform();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyDeleteRandomGaussian() throws Exception {
    super.testKeyDeleteRandomGaussian();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyDelete() throws Exception {
    super.testKeyDelete();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testKeyAddDelete() throws Exception {
    super.testKeyAddDelete();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeFirstEmpty();

    assertFileRestoreFromWAL();
  }

  @Test
  @Override
  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
    super.testAddKeyValuesInTwoBucketsAndMakeLastEmpty();

    assertFileRestoreFromWAL();
  }

  @Test
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
    long sbTreeFileId = actualWriteCache.fileIdByName(sbTree.getName() + ".sbt");
    String nativeSBTreeFileName = actualWriteCache.nativeFileNameById(sbTreeFileId);

    databaseDocumentTx.activateOnCurrentThread();
    databaseDocumentTx.close();
    actualStorage.close(true, false);

    restoreDataFromWAL();

    long expectedSBTreeFileId = expectedWriteCache.fileIdByName("expectedSBTree.sbt");
    String expectedSBTreeNativeFileName = expectedWriteCache.nativeFileNameById(expectedSBTreeFileId);

    expectedDatabaseDocumentTx.activateOnCurrentThread();
    expectedDatabaseDocumentTx.close();
    expectedStorage.close(true, false);

    assertFileContentIsTheSame(expectedSBTreeNativeFileName, nativeSBTreeFileName);
  }

  private void restoreDataFromWAL() throws IOException {
    OCASDiskWriteAheadLog log = new OCASDiskWriteAheadLog(ACTUAL_DB_NAME, Paths.get(actualStorageDir), Paths.get(actualStorageDir),
        10_000, 128, 30 * 60 * 1_000_000_000L, 100 * 1024 * 1024, 1000, false, Locale.ENGLISH, -1, -1, 1_000, false, true, false,
        0);
    OLogSequenceNumber lsn = log.begin();

    List<OWALRecord> atomicUnit = new ArrayList<>();
    List<OWriteableWALRecord> walRecords = log.read(lsn, 1_000);

    boolean atomicChangeIsProcessed = false;
    while (!walRecords.isEmpty()) {
      for (OWriteableWALRecord walRecord : walRecords) {
        if (walRecord instanceof OOperationUnitBodyRecord) {
          atomicUnit.add(walRecord);
        }

        if (!atomicChangeIsProcessed) {
          if (walRecord instanceof OAtomicUnitStartRecord) {
            atomicChangeIsProcessed = true;
          }
        } else if (walRecord instanceof OAtomicUnitEndRecord) {
          atomicChangeIsProcessed = false;

          for (OWALRecord restoreRecord : atomicUnit) {
            if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord
                || restoreRecord instanceof ONonTxOperationPerformedWALRecord) {
              continue;
            }

            if (restoreRecord instanceof OFileCreatedWALRecord) {
              final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord) restoreRecord;
              final String fileName = fileCreatedCreatedRecord.getFileName().replace("actualSBTree", "expectedSBTree");

              if (!expectedWriteCache.exists(fileName)) {
                expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
              }
            } else {
              final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

              final long fileId = updatePageRecord.getFileId();
              final long pageIndex = updatePageRecord.getPageIndex();

              OCacheEntry cacheEntry = expectedReadCache.loadForWrite(fileId, pageIndex, true, expectedWriteCache, 1, false, null);
              if (cacheEntry == null) {
                do {
                  if (cacheEntry != null) {
                    expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
                  }

                  cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache, false, null);
                } while (cacheEntry.getPageIndex() != pageIndex);
              }

              try {
                ODurablePage durablePage = new ODurablePage(cacheEntry);
                durablePage.restoreChanges(updatePageRecord.getChanges());
                durablePage.setLsn(new OLogSequenceNumber(0, 0));

                cacheEntry.markDirty();
              } finally {
                expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
              }
            }

          }
          atomicUnit.clear();
        } else {
          Assert.assertTrue("WAL record type is " + walRecord.getClass().getName(),
              walRecord instanceof OUpdatePageRecord || walRecord instanceof ONonTxOperationPerformedWALRecord
                  || walRecord instanceof OFileCreatedWALRecord || walRecord instanceof OFuzzyCheckpointStartRecord
                  || walRecord instanceof OFuzzyCheckpointEndRecord);
        }
      }

      walRecords = log.next(walRecords.get(walRecords.size() - 1).getLsn(), 1_000);
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();
  }

  private void assertFileContentIsTheSame(String expectedBTreeFileName, String actualBTreeFileName) throws IOException {
    File expectedFile = new File(expectedStorageDir, expectedBTreeFileName);
    try (RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r")) {
      try (RandomAccessFile fileTwo = new RandomAccessFile(new File(actualStorageDir, actualBTreeFileName), "r")) {

        Assert.assertEquals(fileOne.length(), fileTwo.length());

        byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
        byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];

        fileOne.seek(OFileClassic.HEADER_SIZE);
        fileTwo.seek(OFileClassic.HEADER_SIZE);

        int bytesRead = fileOne.read(expectedContent);
        while (bytesRead >= 0) {
          fileTwo.readFully(actualContent, 0, bytesRead);

          Assertions
              .assertThat(Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.MAX_PAGE_SIZE_BYTES))
              .isEqualTo(Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.MAX_PAGE_SIZE_BYTES));
          expectedContent = new byte[OClusterPage.PAGE_SIZE];
          actualContent = new byte[OClusterPage.PAGE_SIZE];
          bytesRead = fileOne.read(expectedContent);
        }

      }
    }
  }
}
