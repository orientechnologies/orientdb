// package com.orientechnologies.orient.core.storage.index.sbtreebonsai.local;
//
// import com.orientechnologies.common.serialization.types.OIntegerSerializer;
// import com.orientechnologies.orient.core.config.OGlobalConfiguration;
// import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
// import com.orientechnologies.orient.core.db.record.OIdentifiable;
// import com.orientechnologies.orient.core.serialization.serializer.binary.impl.OLinkSerializer;
// import com.orientechnologies.orient.core.storage.OStorage;
// import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
// import com.orientechnologies.orient.core.storage.cache.OReadCache;
// import com.orientechnologies.orient.core.storage.cache.OWriteCache;
// import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
// import com.orientechnologies.orient.core.storage.fs.OFileClassic;
// import com.orientechnologies.orient.core.storage.cluster.OClusterPage;
// import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
// import
// com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
// import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
// import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
// import org.junit.*;
//
// import java.io.File;
// import java.io.IOException;
// import java.io.RandomAccessFile;
// import java.util.ArrayList;
// import java.util.Arrays;
// import java.util.List;
//
/// **
// * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
// * @since 8/27/13
// */
// public class OSBTreeBonsaiWALTestIT extends OSBTreeBonsaiLocalTestIT {
//  static {
//    OGlobalConfiguration.FILE_LOCK.setValue(false);
//  }
//
//  private String buildDirectory;
//
//  private String actualStorageDir;
//  private String expectedStorageDir;
//
//  private ODatabaseDocumentTx expectedDatabaseDocumentTx;
//
//  private OReadCache  expectedReadCache;
//  private OWriteCache expectedWriteCache;
//
//  private OLocalPaginatedStorage actualStorage;
//
//  private OAtomicOperationsManager actualAtomicOperationsManager;
//
//  @BeforeClass
//  public static void beforeClass() {
//  }
//
//  @AfterClass
//  public static void afterClass() throws Exception {
//  }
//
//  @Before
//  public void before() throws IOException {
//    buildDirectory = System.getProperty("buildDirectory", ".");
//
//    buildDirectory += "/sbtreeWithWALTest";
//
//    createExpectedSBTree();
//    createActualSBTree();
//  }
//
//  @After
//  @Override
//  public void afterMethod() throws Exception {
//    Assert.assertNull(actualAtomicOperationsManager.getCurrentOperation());
//
//    expectedDatabaseDocumentTx.open("admin", "admin");
//    expectedDatabaseDocumentTx.drop();
//
//    databaseDocumentTx.open("admin", "admin");
//    databaseDocumentTx.drop();
//
//    File ad = new File(actualStorageDir);
//    if (ad.exists())
//      Assert.assertTrue(ad.delete());
//
//    File ed = new File(expectedStorageDir);
//    if (ed.exists())
//      Assert.assertTrue(ed.delete());
//
//    File bd = new File(buildDirectory);
//    if (bd.exists())
//      Assert.assertTrue(bd.delete());
//  }
//
//  private void createActualSBTree() throws IOException {
//    actualStorageDir = buildDirectory + "/sbtreeWithWALTestActual";
//    File buildDir = new File(buildDirectory);
//    if (!buildDir.exists())
//      buildDir.mkdirs();
//
//    File actualStorageDirFile = new File(actualStorageDir);
//    if (!actualStorageDirFile.exists())
//      actualStorageDirFile.mkdirs();
//
//    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + actualStorageDirFile);
//    if (databaseDocumentTx.exists()) {
//      databaseDocumentTx.open("admin", "admin");
//      databaseDocumentTx.drop();
//    }
//
//    databaseDocumentTx.create();
//
//    actualStorage = (OLocalPaginatedStorage) databaseDocumentTx.getStorage();
//    actualAtomicOperationsManager = actualStorage.getAtomicOperationsManager();
//    ODiskWriteAheadLog writeAheadLog = (ODiskWriteAheadLog) actualStorage.getWALInstance();
//
//    actualStorage.synch();
//    writeAheadLog.addCutTillLimit(writeAheadLog.getFlushedLsn());
//
//    sbTree = new OSBTreeBonsaiLocal<Integer, OIdentifiable>("actualSBTree", ".sbt",
// actualStorage);
//    sbTree.create(OIntegerSerializer.INSTANCE, OLinkSerializer.INSTANCE);
//  }
//
//  private void createExpectedSBTree() {
//    expectedStorageDir = buildDirectory + "/sbtreeWithWALTestExpected";
//
//    final File buildDir = new File(buildDirectory);
//    if (!buildDir.exists())
//      buildDir.mkdirs();
//
//    final File expectedStorageDirFile = new File(expectedStorageDir);
//    if (!expectedStorageDirFile.exists())
//      expectedStorageDirFile.mkdirs();
//
//    expectedDatabaseDocumentTx = new ODatabaseDocumentTx("plocal:" + expectedStorageDir);
//    if (expectedDatabaseDocumentTx.exists()) {
//      expectedDatabaseDocumentTx.open("admin", "admin");
//      expectedDatabaseDocumentTx.drop();
//    }
//
//    expectedDatabaseDocumentTx.create();
//
//    final OLocalPaginatedStorage expectedStorage = (OLocalPaginatedStorage)
// expectedDatabaseDocumentTx.getStorage();
//    expectedWriteCache = expectedStorage.getWriteCache();
//    expectedReadCache = expectedStorage.getReadCache();
//
//  }
//
//  @Override
//  public void testKeyPut() throws Exception {
//    super.testKeyPut();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyPutRandomUniform() throws Exception {
//    super.testKeyPutRandomUniform();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyPutRandomGaussian() throws Exception {
//    super.testKeyPutRandomGaussian();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyDelete() throws Exception {
//    super.testKeyDelete();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyDeleteRandomGaussian() throws Exception {
//    super.testKeyDeleteRandomGaussian();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyDelete() throws Exception {
//    super.testKeyDelete();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testKeyAddDelete() throws Exception {
//    super.testKeyAddDelete();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddKeyValuesInTwoBucketsAndMakeFirstEmpty() throws Exception {
//    super.testAddKeyValuesInTwoBucketsAndMakeFirstEmpty();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddKeyValuesInTwoBucketsAndMakeLastEmpty() throws Exception {
//    super.testAddKeyValuesInTwoBucketsAndMakeLastEmpty();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Override
//  public void testAddKeyValuesAndRemoveFirstMiddleAndLastPages() throws Exception {
//    super.testAddKeyValuesAndRemoveFirstMiddleAndLastPages();
//
//    assertFileRestoreFromWAL();
//  }
//
//  @Test
//  @Ignore
//  public void testGetFisrtKeyInEmptyTree() throws Exception {
//    super.testGetFisrtKeyInEmptyTree();
//  }
//
//  @Test
//  @Ignore
//  @Override
//  public void testValuesMajor() {
//    super.testValuesMajor();
//  }
//
//  @Test
//  @Ignore
//  @Override
//  public void testValuesMinor() {
//    super.testValuesMinor();
//  }
//
//  @Test
//  @Ignore
//  @Override
//  public void testValuesBetween() {
//    super.testValuesBetween();
//  }
//
//  private void assertFileRestoreFromWAL() throws IOException {
//    OWOWCache actualWriteCache = (OWOWCache) ((OLocalPaginatedStorage)
// (databaseDocumentTx.getStorage())).getWriteCache();
//
//    final long sbtreeFileId = actualWriteCache.fileIdByName(sbTree.getName() + ".sbt");
//    final String nativeSBTreeFileName = actualWriteCache.nativeFileNameById(sbtreeFileId);
//
//    OStorage storage = databaseDocumentTx.getStorage();
//    databaseDocumentTx.activateOnCurrentThread();
//    databaseDocumentTx.close();
//    storage.close(true, false);
//
//    restoreDataFromWAL();
//
//    final long nativeExpectedSBTreeFileId = expectedWriteCache.fileIdByName("expectedSBTree.sbt");
//    final String nativeExpectedSBTreeFileName = ((OWOWCache)
// expectedWriteCache).nativeFileNameById(nativeExpectedSBTreeFileId);
//
//    expectedDatabaseDocumentTx.activateOnCurrentThread();
//    expectedDatabaseDocumentTx.close();
//    storage = expectedDatabaseDocumentTx.getStorage();
//    storage.close(true, false);
//
//    expectedReadCache.clear();
//
//    assertFileContentIsTheSame(nativeExpectedSBTreeFileName, nativeSBTreeFileName);
//  }
//
//  private void restoreDataFromWAL() throws IOException {
//    ODiskWriteAheadLog log = new ODiskWriteAheadLog(4, -1, 10 * 1024L * OWALPage.PAGE_SIZE, null,
// false, actualStorage,
//        16 * OWALPage.PAGE_SIZE, 120);
//    OLogSequenceNumber lsn = log.begin();
//
//    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();
//
//    boolean atomicChangeIsProcessed = false;
//    while (lsn != null) {
//      OWALRecord walRecord = log.read(lsn);
//      if (walRecord instanceof OOperationUnitBodyRecord)
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
//            final String fileName = fileCreatedCreatedRecord.getFileName().replace("actualSBTree",
// "expectedSBTree");
//
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
//                  expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
//
//                cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache, false);
//              } while (cacheEntry.getPageIndex() != pageIndex);
//            }
//
//            try {
//              ODurablePage durablePage = new ODurablePage(cacheEntry);
//              durablePage.restoreChanges(updatePageRecord.getChanges());
//              durablePage.setOperationIdLsn(new OLogSequenceNumber(0, 0));
//
//            } finally {
//              expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache);
//            }
//          }
//
//        }
//        atomicUnit.clear();
//      } else {
//        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof
// ONonTxOperationPerformedWALRecord
//            || walRecord instanceof OFileCreatedWALRecord || walRecord instanceof
// OFuzzyCheckpointStartRecord
//            || walRecord instanceof OFuzzyCheckpointEndRecord);
//      }
//
//      lsn = log.next(lsn);
//    }
//
//    Assert.assertTrue(atomicUnit.isEmpty());
//    log.close();
//  }
//
//  private void assertFileContentIsTheSame(String expectedBTreeFile, String actualBTreeFile) throws
// IOException {
//    File expectedFile = new File(expectedStorageDir, expectedBTreeFile);
//    RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r");
//    RandomAccessFile fileTwo = new RandomAccessFile(new File(actualStorageDir, actualBTreeFile),
// "r");
//
//    Assert.assertEquals(fileOne.length(), fileTwo.length());
//
//    byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
//    byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];
//
//    fileOne.seek(OFileClassic.HEADER_SIZE);
//    fileTwo.seek(OFileClassic.HEADER_SIZE);
//
//    int bytesRead = fileOne.read(expectedContent);
//    while (bytesRead >= 0) {
//      fileTwo.readFully(actualContent, 0, bytesRead);
//
//      Assert
//          .assertArrayEquals(Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION,
// ODurablePage.MAX_PAGE_SIZE_BYTES),
//              Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION,
// ODurablePage.MAX_PAGE_SIZE_BYTES));
//
//      expectedContent = new byte[OClusterPage.PAGE_SIZE];
//      actualContent = new byte[OClusterPage.PAGE_SIZE];
//      bytesRead = fileOne.read(expectedContent);
//    }
//
//    fileOne.close();
//    fileTwo.close();
//  }
// }
