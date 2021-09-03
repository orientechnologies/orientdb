// package com.orientechnologies.orient.core.storage.impl.local.paginated;
//
// import com.orientechnologies.orient.core.config.OGlobalConfiguration;
// import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
// import com.orientechnologies.orient.core.storage.OStorage;
// import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
// import com.orientechnologies.orient.core.storage.cache.OReadCache;
// import com.orientechnologies.orient.core.storage.cache.OWriteCache;
// import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
// import com.orientechnologies.orient.core.storage.fs.OFileClassic;
// import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
// import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
// import
// com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
// import org.junit.*;
//
// import java.io.File;
// import java.io.IOException;
// import java.io.RandomAccessFile;
// import java.lang.reflect.Array;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
//
// import static org.assertj.core.api.Assertions.assertThat;
//
/// **
// * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
// * @since 5/8/13
// */
//
// public class LocalPaginatedClusterWithWALTestIT extends LocalPaginatedClusterTestIT {
//  static {
//    OGlobalConfiguration.FILE_LOCK.setValue(false);
//  }
//
//  private ODatabaseDocumentTx expectedDatabase;
//
//  private OReadCache  readCache;
//  private OWriteCache writeCache;
//
//  private OReadCache  expectedReadCache;
//  private OWriteCache expectedWriteCache;
//
//  private String                 storageDir;
//  private String                 expectedStorageDir;
//  private OLocalPaginatedStorage storage;
//
//  @BeforeClass
//  public static void beforeClass() throws IOException {
//  }
//
//  @AfterClass
//  public static void afterClass() throws IOException {
//  }
//
//  @Before
//  @Override
//  public void before() throws IOException {
//    buildDirectory = System.getProperty("buildDirectory", ".");
//    buildDirectory += "/localPaginatedClusterWithWALTest";
//
//    File buildDir = new File(buildDirectory);
//    if (!buildDir.exists())
//      buildDir.mkdirs();
//
//    createActualStorage();
//
//    createExpectedStorage();
//    createPaginatedCluster();
//  }
//
//  private void createExpectedStorage() {
//    expectedDatabase = new ODatabaseDocumentTx("plocal:" + buildDirectory + File.separator +
// "localPaginatedClusterWithWALTestTwo");
//    if (expectedDatabase.exists()) {
//      expectedDatabase.open("admin", "admin");
//      expectedDatabase.drop();
//    }
//
//    expectedDatabase.create();
//    OLocalPaginatedStorage expectedStorage = (OLocalPaginatedStorage)
// expectedDatabase.getStorage();
//    expectedWriteCache = expectedStorage.getWriteCache();
//    expectedReadCache = expectedStorage.getReadCache();
//
//    expectedStorageDir = expectedStorage.getStoragePath().toString();
//  }
//
//  private void createActualStorage() throws IOException {
//    databaseDocumentTx = new ODatabaseDocumentTx(
//        "plocal:" + buildDirectory + File.separator + "localPaginatedClusterWithWALTestOne");
//    if (databaseDocumentTx.exists()) {
//      databaseDocumentTx.open("admin", "admin");
//      databaseDocumentTx.drop();
//    }
//
//    databaseDocumentTx.create();
//    storage = (OLocalPaginatedStorage) databaseDocumentTx.getStorage();
//
//    storage.synch();
//    ODiskWriteAheadLog writeAheadLog = (ODiskWriteAheadLog) storage.getWALInstance();
//    writeAheadLog.addCutTillLimit(writeAheadLog.getFlushedLsn());
//    writeCache = storage.getWriteCache();
//    readCache = storage.getReadCache();
//
//    storageDir = storage.getStoragePath().toString();
//  }
//
//  private void createPaginatedCluster() throws IOException {
//    paginatedCluster = new OPaginatedCluster("actualPaginatedClusterWithWALTest", storage);
//    paginatedCluster.configure(storage, 42, "actualPaginatedClusterWithWALTest", buildDirectory,
// -1);
//    paginatedCluster.create(-1);
//  }
//
//  @After
//  public void afterMethod() throws IOException {
//    expectedDatabase.open("admin", "admin");
//    expectedDatabase.drop();
//
//    databaseDocumentTx.open("admin", "admin");
//    databaseDocumentTx.drop();
//
//    File file = new File(storageDir);
//    if (file.exists())
//      Assert.assertTrue(file.delete());
//
//    file = new File(expectedStorageDir);
//    if (file.exists())
//      Assert.assertTrue(file.delete());
//
//    file = new File(buildDirectory);
//    if (file.exists())
//      Assert.assertTrue(file.delete());
//  }
//
//  @Test
//  @Override
//  public void testAddOneSmallRecord() throws IOException {
//    super.testAddOneSmallRecord();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddOneBigRecord() throws IOException {
//    super.testAddOneBigRecord();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateOneSmallRecord() throws IOException {
//    super.testUpdateOneSmallRecord();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateOneBigRecord() throws IOException {
//    super.testUpdateOneBigRecord();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testDeleteRecordAndAddNewOnItsPlace() throws IOException {
//    super.testDeleteRecordAndAddNewOnItsPlace();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddManySmallRecords() throws IOException {
//    super.testAddManySmallRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateOneSmallRecordVersionIsLowerCurrentOne() throws IOException {
//    super.testUpdateOneSmallRecordVersionIsLowerCurrentOne();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateOneSmallRecordVersionIsMinusTwo() throws IOException {
//    super.testUpdateOneSmallRecordVersionIsMinusTwo();
//
//    assertFileRestoreFromWAL();
//  }
//
//  public void testResurrectRecord() throws IOException {
//    super.testResurrectRecord();
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateManySmallRecords() throws IOException {
//    super.testUpdateManySmallRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddManyRecords() throws IOException {
//    super.testAddManyRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddManyBigRecords() throws IOException {
//    super.testAddManyBigRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateManyRecords() throws IOException {
//    super.testUpdateManyRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testUpdateManyBigRecords() throws IOException {
//    super.testUpdateManyBigRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testRemoveHalfSmallRecords() throws IOException {
//    super.testRemoveHalfSmallRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Test
//  @Override
//  public void testRemoveHalfRecords() throws IOException {
//    super.testRemoveHalfRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testRemoveHalfBigRecords() throws IOException {
//    super.testRemoveHalfBigRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testRemoveHalfRecordsAndAddAnotherHalfAgain() throws IOException {
//    super.testRemoveHalfRecordsAndAddAnotherHalfAgain();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testHideHalfSmallRecords() throws IOException {
//    super.testHideHalfSmallRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testHideHalfBigRecords() throws IOException {
//    super.testHideHalfBigRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testHideHalfRecords() throws IOException {
//    super.testHideHalfRecords();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testHideHalfRecordsAndAddAnotherHalfAgain() throws IOException {
//    super.testHideHalfRecordsAndAddAnotherHalfAgain();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAllocatePositionMap() throws IOException {
//    super.testAllocatePositionMap();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testManyAllocatePositionMap() throws IOException {
//    super.testManyAllocatePositionMap();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  @Test
//  @Ignore
//  public void testForwardIteration() throws IOException {
//    super.testForwardIteration();
//  }
//
//  @Override
//  @Test
//  @Ignore
//  public void testBackwardIteration() throws IOException {
//    super.testBackwardIteration();
//  }
//
//  @Override
//  @Test
//  @Ignore
//  public void testGetPhysicalPosition() throws IOException {
//    super.testGetPhysicalPosition();
//  }
//
//  private void assertFileRestoreFromWAL() throws IOException {
//    long actualDataFileId = writeCache.fileIdByName(paginatedCluster.getName() + ".pcl");
//    String actualDataFileNativeFileName = ((OWOWCache)
// writeCache).nativeFileNameById(actualDataFileId);
//
//    long actualClusterPositionMapId = writeCache.fileIdByName(paginatedCluster.getName() +
// ".cpm");
//    String actualClusterPositionMapName = ((OWOWCache)
// writeCache).nativeFileNameById(actualClusterPositionMapId);
//
//    databaseDocumentTx.activateOnCurrentThread();
//    OStorage storage = databaseDocumentTx.getStorage();
//    databaseDocumentTx.close();
//    storage.close(true, false);
//
//    restoreClusterFromWAL();
//
//    long expectedDataFileId =
// expectedWriteCache.fileIdByName("expectedPaginatedClusterWithWALTest.pcl");
//    String expectedDataFileNativeFileName = ((OWOWCache)
// expectedWriteCache).nativeFileNameById(expectedDataFileId);
//
//    long expectedClusterPositionMapId =
// expectedWriteCache.fileIdByName("expectedPaginatedClusterWithWALTest.cpm");
//    String expectedClusterPositionMapName = ((OWOWCache)
// expectedWriteCache).nativeFileNameById(expectedClusterPositionMapId);
//
//    expectedDatabase.activateOnCurrentThread();
//    storage = expectedDatabase.getStorage();
//    expectedDatabase.close();
//    storage.close(true, false);
//
//    assertClusterContentIsTheSame(expectedDataFileNativeFileName, actualDataFileNativeFileName,
// expectedClusterPositionMapName,
//        actualClusterPositionMapName);
//  }
//
//  private void restoreClusterFromWAL() throws IOException {
//    ODiskWriteAheadLog log = new ODiskWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE,
//        null, true, storage, 32 * 1024 * 1024,
//        120);
//    OLogSequenceNumber lsn = log.begin();
//
//    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();
//
//    boolean atomicChangeIsProcessed = false;
//    while (lsn != null) {
//      OWALRecord walRecord = log.read(lsn);
//      if (walRecord instanceof OOperationUnitRecord)
//        atomicUnit.add(walRecord);
//
//      if (!atomicChangeIsProcessed) {
//        if (walRecord instanceof OAtomicUnitStartRecord)
//          atomicChangeIsProcessed = true;
//      } else if (walRecord instanceof OAtomicUnitEndRecord) {
//        atomicChangeIsProcessed = false;
//
//        for (OWALRecord restoreRecord : atomicUnit) {
//          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof
// OAtomicUnitEndRecord
//              || restoreRecord instanceof ONonTxOperationPerformedWALRecord)
//            continue;
//
//          if (restoreRecord instanceof OFileCreatedWALRecord) {
//            final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord)
// restoreRecord;
//            final String fileName = fileCreatedCreatedRecord.getFileName()
//                .replace("actualPaginatedClusterWithWALTest",
// "expectedPaginatedClusterWithWALTest");
//            if (!expectedWriteCache.exists(fileName))
//              expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(),
// expectedWriteCache);
//          } else {
//            final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;
//
//            final long fileId = updatePageRecord.getFileId();
//            final long pageIndex = updatePageRecord.getPageIndex();
//
//            OCacheEntry cacheEntry = expectedReadCache.loadForWrite(fileId, pageIndex, true,
// expectedWriteCache, 1, false);
//            if (cacheEntry == null) {
//              do {
//                if (cacheEntry != null)
//                  readCache.releaseFromWrite(cacheEntry, expectedWriteCache);
//
//                cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache, false);
//              } while (cacheEntry.getPageIndex() != pageIndex);
//            }
//            try {
//              ODurablePage durablePage = new ODurablePage(cacheEntry);
//              durablePage.restoreChanges(updatePageRecord.getChanges());
//              durablePage.setOperationIdLsn(new OLogSequenceNumber(0, 0));
//
//            } finally {
//              expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
//            }
//          }
//        }
//
//        atomicUnit.clear();
//      } else {
//        Assert.assertTrue("Unexpected type of the WAL record " + walRecord.getClass().getName(),
//            walRecord instanceof OUpdatePageRecord || walRecord instanceof OFileCreatedWALRecord
//                || walRecord instanceof ONonTxOperationPerformedWALRecord);
//      }
//
//      lsn = log.next(lsn);
//    }
//
//    Assert.assertTrue(atomicUnit.isEmpty());
//    log.close();
//  }
//
//  private void assertClusterContentIsTheSame(String expectedDataFileName, String
// actualDataFileName,
//      String expectedClusterPositionMapName, String actualClusterPositionMapMap) throws
// IOException {
//
//    File expectedDataFile = new File(expectedStorageDir, expectedDataFileName);
//    RandomAccessFile datFileOne = new RandomAccessFile(expectedDataFile, "r");
//    RandomAccessFile datFileTwo = new RandomAccessFile(new File(storageDir, actualDataFileName),
// "r");
//
//    assertFileContentIsTheSame(datFileOne, datFileTwo);
//
//    datFileOne.close();
//    datFileTwo.close();
//
//    File expectedRIDMapFile = new File(expectedStorageDir, expectedClusterPositionMapName);
//    RandomAccessFile ridMapOne = new RandomAccessFile(expectedRIDMapFile, "r");
//    RandomAccessFile ridMapTwo = new RandomAccessFile(new File(storageDir,
// actualClusterPositionMapMap), "r");
//
//    assertFileContentIsTheSame(ridMapOne, ridMapTwo);
//
//    ridMapOne.close();
//    ridMapTwo.close();
//
//  }
//
//  private void assertFileContentIsTheSame(RandomAccessFile datFileOne, RandomAccessFile
// datFileTwo) throws IOException {
//    Assert.assertEquals(datFileOne.length(), datFileTwo.length());
//
//    byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
//    byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];
//
//    datFileOne.seek(OFileClassic.HEADER_SIZE);
//    datFileTwo.seek(OFileClassic.HEADER_SIZE);
//
//    int bytesRead = datFileOne.read(expectedContent);
//    while (bytesRead >= 0) {
//      datFileTwo.readFully(actualContent, 0, bytesRead);
//
//      //      Assert.assertEquals(expectedContent, actualContent);
//
//      assertThat(Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION,
// ODurablePage.MAX_PAGE_SIZE_BYTES))
//          .isEqualTo(Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION,
// ODurablePage.MAX_PAGE_SIZE_BYTES));
//
//      expectedContent = new byte[OClusterPage.PAGE_SIZE];
//      actualContent = new byte[OClusterPage.PAGE_SIZE];
//      bytesRead = datFileOne.read(expectedContent);
//    }
//  }
// }
