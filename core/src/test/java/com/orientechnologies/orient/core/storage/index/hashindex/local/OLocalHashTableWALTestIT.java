//package com.orientechnologies.orient.core.storage.index.hashindex.local;
//
//import com.orientechnologies.common.io.OFileUtils;
//import com.orientechnologies.common.serialization.types.OIntegerSerializer;
//import com.orientechnologies.orient.core.db.ODatabaseInternal;
//import com.orientechnologies.orient.core.db.ODatabaseSession;
//import com.orientechnologies.orient.core.db.ODatabaseType;
//import com.orientechnologies.orient.core.db.OrientDB;
//import com.orientechnologies.orient.core.db.OrientDBConfig;
//import com.orientechnologies.orient.core.metadata.schema.OType;
//import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
//import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
//import com.orientechnologies.orient.core.storage.cache.OReadCache;
//import com.orientechnologies.orient.core.storage.cache.OWriteCache;
//import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
//import com.orientechnologies.orient.core.storage.fs.OFileClassic;
//import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
//import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
//import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitEndRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAtomicUnitStartRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OCheckpointEndRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFileCreatedWALRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFullCheckpointStartRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointEndRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OFuzzyCheckpointStartRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ONonTxOperationPerformedWALRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OOperationUnitBodyRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OUpdatePageRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecord;
//import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
//import org.junit.After;
//import org.junit.Assert;
//import org.junit.Before;
//
//import java.io.File;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.util.ArrayList;
//import java.util.Arrays;
//import java.util.List;
//
///**
// * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
// * @since 5/19/14
// */
//public class OLocalHashTableWALTestIT extends OLocalHashTableV2Base {
//  private static final String ACTUAL_DB_NAME   = OLocalHashTableWALTestIT.class.getSimpleName() + "Actual";
//  private static final String EXPECTED_DB_NAME = OLocalHashTableWALTestIT.class.getSimpleName() + "Expected";
//
//  private String actualStorageDir;
//  private String expectedStorageDir;
//
//  private ODatabaseSession databaseDocumentTx;
//
//  private OWOWCache actualWriteCache;
//  private OWOWCache expectedWriteCache;
//
//  private ODatabaseSession expectedDatabaseDocumentTx;
//
//  private OrientDB orientDB;
//
//  @Before
//  public void before() {
//    String buildDirectory = System.getProperty("buildDirectory", ".");
//
//    buildDirectory += "/" + this.getClass().getSimpleName();
//
//    final File buildDir = new File(buildDirectory);
//    OFileUtils.deleteRecursively(buildDir);
//
//    orientDB = new OrientDB("plocal:" + buildDirectory, OrientDBConfig.defaultConfig());
//
//    orientDB.create(ACTUAL_DB_NAME, ODatabaseType.PLOCAL);
//    databaseDocumentTx = orientDB.open(ACTUAL_DB_NAME, "admin", "admin");
//
//    orientDB.create(EXPECTED_DB_NAME, ODatabaseType.PLOCAL);
//    expectedDatabaseDocumentTx = orientDB.open(EXPECTED_DB_NAME, "admin", "admin");
//
//    actualStorageDir = ((OLocalPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getStoragePath().toString();
//    expectedStorageDir = ((OLocalPaginatedStorage) ((ODatabaseInternal) expectedDatabaseDocumentTx).getStorage()).getStoragePath()
//        .toString();
//
//    actualWriteCache = (OWOWCache) ((OLocalPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getWriteCache();
//    expectedWriteCache = (OWOWCache) ((OLocalPaginatedStorage) ((ODatabaseInternal) expectedDatabaseDocumentTx).getStorage())
//        .getWriteCache();
//
//    OLocalPaginatedStorage actualStorage = (OLocalPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage();
//    ODiskWriteAheadLog diskWriteAheadLog = (ODiskWriteAheadLog) actualStorage.getWALInstance();
//
//    actualStorage.synch();
//    diskWriteAheadLog.addCutTillLimit(diskWriteAheadLog.getFlushedLsn());
//
//    createActualHashTable();
//  }
//
//  @After
//  public void after() {
//    orientDB.drop(ACTUAL_DB_NAME);
//    orientDB.drop(EXPECTED_DB_NAME);
//    orientDB.close();
//  }
//
//  private void createActualHashTable() {
//    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<>(OIntegerSerializer.INSTANCE);
//
//    localHashTable = new OLocalHashTableV2<>("actualLocalHashTable", ".imc", ".tsc", ".obf", ".nbh",
//        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
//    localHashTable
//        .create(OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance().getObjectSerializer(OType.STRING), null, null,
//            murmurHash3HashFunction, true);
//  }
//
//  @Override
//  public void testKeyPut() throws IOException {
//    super.testKeyPut();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyPutRandomUniform() throws IOException {
//    super.testKeyPutRandomUniform();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyPutRandomGaussian() throws IOException {
//    super.testKeyPutRandomGaussian();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyDelete() throws IOException {
//    super.testKeyDelete();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyDelete() throws IOException {
//    super.testKeyDelete();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyDeleteRandomGaussian() throws IOException {
//    super.testKeyDeleteRandomGaussian();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyAddDelete() throws IOException {
//    super.testKeyAddDelete();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyPutRemoveNullKey() throws IOException {
//    super.testKeyPutRemoveNullKey();
//
//    Assert.assertNull(
//        ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getAtomicOperationsManager()
//            .getCurrentOperation());
//
//    assertFileRestoreFromWAL();
//  }
//
//  private void assertFileRestoreFromWAL() throws IOException {
//    final long imcFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".imc");
//    final String nativeImcFileName = actualWriteCache.nativeFileNameById(imcFileId);
//
//    final long tscFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".tsc");
//    final String nativeTscFileName = actualWriteCache.nativeFileNameById(tscFileId);
//
//    final long nbhFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".nbh");
//    final String nativeNBHFileName = actualWriteCache.nativeFileNameById(nbhFileId);
//
//    final long obfFileId = actualWriteCache.fileIdByName(localHashTable.getName() + ".obf");
//    final String nativeOBFFileName = actualWriteCache.nativeFileNameById(obfFileId);
//
//    localHashTable.close();
//
//    System.out.println("Start data restore");
//    restoreDataFromWAL();
//    System.out.println("Stop data restore");
//
//    final long expectedImcFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.imc");
//    final String nativeExpectedImcFileName = expectedWriteCache.nativeFileNameById(expectedImcFileId);
//
//    final long expectedTscFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.tsc");
//    final String nativeExpectedTscFileName = expectedWriteCache.nativeFileNameById(expectedTscFileId);
//
//    final long expectedNbhFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.nbh");
//    final String nativeExpectedNBHFileName = expectedWriteCache.nativeFileNameById(expectedNbhFileId);
//
//    final long expectedObfFileId = expectedWriteCache.fileIdByName("expectedLocalHashTable.obf");
//    final String nativeExpectedOBFFile = expectedWriteCache.nativeFileNameById(expectedObfFileId);
//
//    databaseDocumentTx.activateOnCurrentThread();
//    databaseDocumentTx.close();
//    expectedDatabaseDocumentTx.activateOnCurrentThread();
//    expectedDatabaseDocumentTx.close();
//
//    System.out.println("Start data comparison");
//
//    assertFileContentIsTheSame(nativeExpectedImcFileName, nativeImcFileName, nativeExpectedTscFileName, nativeTscFileName,
//        nativeExpectedNBHFileName, nativeNBHFileName, nativeExpectedOBFFile, nativeOBFFileName);
//
//    System.out.println("Stop data comparison");
//  }
//
//  private void restoreDataFromWAL() throws IOException {
//    OWriteAheadLog log = ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage()).getWALInstance();
//
//    OLogSequenceNumber lsn = log.begin();
//
//    List<OWALRecord> atomicUnit = new ArrayList<>();
//    List<OWALRecord> batch = new ArrayList<>();
//
//    boolean atomicChangeIsProcessed = false;
//    while (lsn != null) {
//      OWALRecord walRecord = log.read(lsn);
//      batch.add(walRecord);
//
//      if (batch.size() >= 1000) {
//        atomicChangeIsProcessed = restoreDataFromBatch(atomicChangeIsProcessed, atomicUnit, batch);
//        batch = new ArrayList<>();
//      }
//
//      lsn = log.next(lsn);
//    }
//
//    if (batch.size() > 0) {
//      restoreDataFromBatch(atomicChangeIsProcessed, atomicUnit, batch);
//    }
//
//    Assert.assertTrue(atomicUnit.isEmpty());
//
//    OWriteCache writeCache = ((OAbstractPaginatedStorage) ((ODatabaseInternal) expectedDatabaseDocumentTx).getStorage())
//        .getWriteCache();
//    writeCache.flush();
//  }
//
//  private boolean restoreDataFromBatch(boolean atomicChangeIsProcessed, List<OWALRecord> atomicUnit, List<OWALRecord> records)
//      throws IOException {
//
//    final OReadCache expectedReadCache = ((OAbstractPaginatedStorage) ((ODatabaseInternal) expectedDatabaseDocumentTx).getStorage())
//        .getReadCache();
//    final OWriteCache expectedWriteCache = ((OAbstractPaginatedStorage) ((ODatabaseInternal) expectedDatabaseDocumentTx)
//        .getStorage()).getWriteCache();
//
//    for (OWALRecord walRecord : records) {
//      if (walRecord instanceof OOperationUnitBodyRecord)
//        atomicUnit.add(walRecord);
//
//      if (!atomicChangeIsProcessed && walRecord instanceof OAtomicUnitStartRecord) {
//        atomicChangeIsProcessed = true;
//      } else if (walRecord instanceof OAtomicUnitEndRecord) {
//        atomicChangeIsProcessed = false;
//
//        for (OWALRecord restoreRecord : atomicUnit) {
//          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord
//              || restoreRecord instanceof ONonTxOperationPerformedWALRecord || restoreRecord instanceof OFullCheckpointStartRecord
//              || restoreRecord instanceof OCheckpointEndRecord)
//            continue;
//
//          if (restoreRecord instanceof OUpdatePageRecord) {
//            final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;
//
//            final long fileId = updatePageRecord.getFileId();
//            final long pageIndex = updatePageRecord.getPageIndex();
//
//            OCacheEntry cacheEntry = expectedReadCache.loadForWrite(fileId, pageIndex, true, expectedWriteCache, 1, false);
//            if (cacheEntry == null)
//              do {
//                cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache, false);
//              } while (cacheEntry.getPageIndex() != pageIndex);
//
//            try {
//              ODurablePage durablePage = new ODurablePage(cacheEntry);
//              durablePage.restoreChanges(updatePageRecord.getChanges());
//              durablePage.setLsn(new OLogSequenceNumber(0, 0));
//            } finally {
//              expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
//            }
//          } else if (restoreRecord instanceof OFileCreatedWALRecord) {
//            final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord) restoreRecord;
//            String fileName = fileCreatedCreatedRecord.getFileName().replace("actualLocalHashTable", "expectedLocalHashTable");
//
//            if (!expectedWriteCache.exists(fileName))
//              expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
//          }
//        }
//
//        atomicUnit.clear();
//      } else {
//        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof OFileCreatedWALRecord
//            || walRecord instanceof ONonTxOperationPerformedWALRecord || walRecord instanceof OFullCheckpointStartRecord
//            || walRecord instanceof OCheckpointEndRecord || walRecord instanceof OFuzzyCheckpointStartRecord
//            || walRecord instanceof OFuzzyCheckpointEndRecord);
//      }
//
//    }
//
//    return atomicChangeIsProcessed;
//  }
//
//  private void assertFileContentIsTheSame(String expectedIMCFile, String actualIMCFile, String expectedTSCFile,
//      String actualTSCFile, String expectedNBHFile, String actualNBHFile, String expectedOBFFile, String actualOBFFile)
//      throws IOException {
//
//    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedIMCFile), new File(actualStorageDir, actualIMCFile));
//    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedTSCFile), new File(actualStorageDir, actualTSCFile));
//    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedNBHFile), new File(actualStorageDir, actualNBHFile));
//    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedOBFFile), new File(actualStorageDir, actualOBFFile));
//  }
//
//  private void assertCompareFilesAreTheSame(File expectedFile, File actualFile) throws IOException {
//    try (RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r")) {
//      try (RandomAccessFile fileTwo = new RandomAccessFile(actualFile, "r")) {
//
//        Assert.assertEquals(fileOne.length(), fileTwo.length());
//
//        byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
//        byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];
//
//        fileOne.seek(OFileClassic.HEADER_SIZE);
//        fileTwo.seek(OFileClassic.HEADER_SIZE);
//
//        int bytesRead = fileOne.read(expectedContent);
//        while (bytesRead >= 0) {
//          fileTwo.readFully(actualContent, 0, bytesRead);
//
//          Assert.assertArrayEquals(
//              Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.MAX_PAGE_SIZE_BYTES),
//              Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.MAX_PAGE_SIZE_BYTES));
//
//          expectedContent = new byte[OClusterPage.PAGE_SIZE];
//          actualContent = new byte[OClusterPage.PAGE_SIZE];
//          bytesRead = fileOne.read(expectedContent);
//        }
//      }
//    }
//  }
//}
