package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/19/14
 */
public class LocalHashTableV2WALTestIT {
  //  private static final String ACTUAL_DB_NAME   = LocalHashTableV2WALTestIT.class.getSimpleName()
  // + "Actual";
  //  private static final String EXPECTED_DB_NAME = LocalHashTableV2WALTestIT.class.getSimpleName()
  // + "Expected";
  //
  //  private OLocalPaginatedStorage actualStorage;
  //  private OLocalPaginatedStorage expectedStorage;
  //
  //  private String actualStorageDir;
  //  private String expectedStorageDir;
  //
  //  private ODatabaseSession databaseDocumentTx;
  //
  //  private OWriteCache actualWriteCache;
  //
  //  private ODatabaseSession expectedDatabaseDocumentTx;
  //  private OWriteCache      expectedWriteCache;
  //
  //  private OrientDB orientDB;
  //
  //  @Before
  //  public void before() throws IOException {
  //    String buildDirectory = System.getProperty("buildDirectory", ".");
  //
  //    buildDirectory += "/" + this.getClass().getSimpleName();
  //
  //    final java.io.File buildDir = new java.io.File(buildDirectory);
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
  //    expectedStorage = ((OLocalPaginatedStorage) ((ODatabaseInternal)
  // expectedDatabaseDocumentTx).getStorage());
  //    actualStorage = (OLocalPaginatedStorage) ((ODatabaseInternal)
  // databaseDocumentTx).getStorage();
  //
  //    actualStorageDir = actualStorage.getStoragePath().toString();
  //    expectedStorageDir = expectedStorage.getStoragePath().toString();
  //
  //    actualWriteCache = ((OLocalPaginatedStorage) ((ODatabaseInternal)
  // databaseDocumentTx).getStorage()).getWriteCache();
  //    expectedWriteCache = ((OLocalPaginatedStorage) ((ODatabaseInternal)
  // expectedDatabaseDocumentTx).getStorage()).getWriteCache();
  //
  //    CASDiskWriteAheadLog diskWriteAheadLog = (CASDiskWriteAheadLog)
  // actualStorage.getWALInstance();
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
  //  private void createActualHashTable() throws IOException {
  //    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new
  // OMurmurHash3HashFunction<>(OIntegerSerializer.INSTANCE);
  //
  //    localHashTable = new LocalHashTableV2<>(42, "actualLocalHashTable", ".imc", ".tsc", ".obf",
  // ".nbh",
  //        (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());
  //    localHashTable
  //        .create(OIntegerSerializer.INSTANCE,
  // OBinarySerializerFactory.getInstance().getObjectSerializer(OType.STRING), null, null,
  //            murmurHash3HashFunction, true);
  //  }
  //
  //  @Override
  //  public void testKeyPut() throws IOException {
  //    super.testKeyPut();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
  //
  //    assertFileRestoreFromWAL();
  //  }
  //
  //  @Override
  //  public void testKeyPutRandomUniform() throws IOException {
  //    super.testKeyPutRandomUniform();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
  //
  //    assertFileRestoreFromWAL();
  //  }
  //
  //  @Override
  //  public void testKeyPutRandomGaussian() throws IOException {
  //    super.testKeyPutRandomGaussian();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
  //
  //    assertFileRestoreFromWAL();
  //  }
  //
  //  @Override
  //  public void testKeyDelete() throws IOException {
  //    super.testKeyDelete();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
  //
  //    assertFileRestoreFromWAL();
  //  }
  //
  //  @Override
  //  public void testKeyDeleteRandomGaussian() throws IOException {
  //    super.testKeyDeleteRandomGaussian();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
  //
  //    assertFileRestoreFromWAL();
  //  }
  //
  //  @Override
  //  public void testKeyAddDelete() throws IOException {
  //    super.testKeyAddDelete();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
  //
  //    assertFileRestoreFromWAL();
  //  }
  //
  //  @Override
  //  public void testKeyPutRemoveNullKey() throws IOException {
  //    super.testKeyPutRemoveNullKey();
  //
  //    Assert.assertNull(OAtomicOperationsManager.getCurrentOperation());
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
  //    databaseDocumentTx.activateOnCurrentThread();
  //    databaseDocumentTx.close();
  //    actualStorage.close(true, false);
  //
  //    System.out.println("Start data restore");
  //    restoreDataFromWAL();
  //    System.out.println("Stop data restore");
  //
  //    final long expectedImcFileId =
  // expectedWriteCache.fileIdByName("expectedLocalHashTable.imc");
  //    final String nativeExpectedImcFileName =
  // expectedWriteCache.nativeFileNameById(expectedImcFileId);
  //
  //    final long expectedTscFileId =
  // expectedWriteCache.fileIdByName("expectedLocalHashTable.tsc");
  //    final String nativeExpectedTscFileName =
  // expectedWriteCache.nativeFileNameById(expectedTscFileId);
  //
  //    final long expectedNbhFileId =
  // expectedWriteCache.fileIdByName("expectedLocalHashTable.nbh");
  //    final String nativeExpectedNBHFileName =
  // expectedWriteCache.nativeFileNameById(expectedNbhFileId);
  //
  //    final long expectedObfFileId =
  // expectedWriteCache.fileIdByName("expectedLocalHashTable.obf");
  //    final String nativeExpectedOBFFile =
  // expectedWriteCache.nativeFileNameById(expectedObfFileId);
  //
  //    expectedDatabaseDocumentTx.activateOnCurrentThread();
  //    expectedDatabaseDocumentTx.close();
  //    expectedStorage.close(true, false);
  //
  //    System.out.println("Start data comparison");
  //
  //    assertFileContentIsTheSame(nativeExpectedImcFileName, nativeImcFileName,
  // nativeExpectedTscFileName, nativeTscFileName,
  //        nativeExpectedNBHFileName, nativeNBHFileName, nativeExpectedOBFFile, nativeOBFFileName);
  //
  //    System.out.println("Stop data comparison");
  //  }
  //
  //  private void restoreDataFromWAL() throws IOException {
  //    final OReadCache expectedReadCache = ((OAbstractPaginatedStorage) ((ODatabaseInternal)
  // expectedDatabaseDocumentTx).getStorage())
  //        .getReadCache();
  //
  //    CASDiskWriteAheadLog log = new CASDiskWriteAheadLog(ACTUAL_DB_NAME,
  // Paths.get(actualStorageDir), Paths.get(actualStorageDir),
  //        10_000, 128, null, null, 30 * 60 * 1_000_000_000L, 100 * 1024 * 1024, 1000, false,
  // Locale.ENGLISH, -1, -1, 1_000, false,
  //        true, false, 0);
  //    OLogSequenceNumber lsn = log.begin();
  //
  //    List<OWALRecord> atomicUnit = new ArrayList<>();
  //    List<WriteableWALRecord> walRecords = log.read(lsn, 1_000);
  //
  //    boolean atomicChangeIsProcessed = false;
  //    while (!walRecords.isEmpty()) {
  //      for (WriteableWALRecord walRecord : walRecords) {
  //        if (walRecord instanceof OOperationUnitBodyRecord) {
  //          atomicUnit.add(walRecord);
  //        }
  //
  //        if (!atomicChangeIsProcessed) {
  //          if (walRecord instanceof OAtomicUnitStartRecord) {
  //            atomicChangeIsProcessed = true;
  //          }
  //        } else if (walRecord instanceof OAtomicUnitEndRecord) {
  //          atomicChangeIsProcessed = false;
  //
  //          for (OWALRecord restoreRecord : atomicUnit) {
  //            if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof
  // OAtomicUnitEndRecord
  //                || restoreRecord instanceof ONonTxOperationPerformedWALRecord) {
  //              continue;
  //            }
  //
  //            if (restoreRecord instanceof OFileCreatedWALRecord) {
  //              final OFileCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedWALRecord)
  // restoreRecord;
  //              final String fileName = fileCreatedCreatedRecord.getFileName().
  //                  replace("actualLocalHashTable", "expectedLocalHashTable");
  //
  //              if (!expectedWriteCache.exists(fileName)) {
  //                expectedReadCache.addFile(fileName, fileCreatedCreatedRecord.getFileId(),
  // expectedWriteCache);
  //              }
  //            } else {
  //              final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;
  //
  //              final long fileId = updatePageRecord.getFileId();
  //              final long pageIndex = updatePageRecord.getPageIndex();
  //
  //              if (!expectedWriteCache.exists(fileId)) {
  //                //some files can be absent for example configuration files
  //                continue;
  //              }
  //
  //              OCacheEntry cacheEntry = expectedReadCache.loadForWrite(fileId, pageIndex, true,
  // expectedWriteCache, false, null);
  //              if (cacheEntry == null) {
  //                do {
  //                  if (cacheEntry != null) {
  //                    expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache, true);
  //                  }
  //
  //                  cacheEntry = expectedReadCache.allocateNewPage(fileId, expectedWriteCache,
  // null);
  //                } while (cacheEntry.getPageIndex() != pageIndex);
  //              }
  //
  //              try {
  //                ODurablePage durablePage = new ODurablePage(cacheEntry);
  //                durablePage.restoreChanges(updatePageRecord.getChanges());
  //                durablePage.setOperationIdLsn(new OLogSequenceNumber(0, 0));
  //              } finally {
  //                expectedReadCache.releaseFromWrite(cacheEntry, expectedWriteCache, true);
  //              }
  //            }
  //
  //          }
  //          atomicUnit.clear();
  //        } else {
  //          Assert.assertTrue("WAL record type is " + walRecord.getClass().getName(),
  //              walRecord instanceof OUpdatePageRecord || walRecord instanceof
  // ONonTxOperationPerformedWALRecord
  //                  || walRecord instanceof OFileCreatedWALRecord || walRecord instanceof
  // OFuzzyCheckpointStartRecord
  //                  || walRecord instanceof OFuzzyCheckpointEndRecord);
  //        }
  //      }
  //
  //      walRecords = log.next(walRecords.get(walRecords.size() - 1).getLsn(), 1_000);
  //    }
  //
  //    Assert.assertTrue(atomicUnit.isEmpty());
  //    log.close();
  //  }
  //
  //  private void assertFileContentIsTheSame(String expectedIMCFile, String actualIMCFile, String
  // expectedTSCFile,
  //      String actualTSCFile, String expectedNBHFile, String actualNBHFile, String
  // expectedOBFFile, String actualOBFFile)
  //      throws IOException {
  //
  //    assertFileContentIsTheSame(new java.io.File(expectedStorageDir,
  // expectedIMCFile).getAbsolutePath(),
  //        new java.io.File(actualStorageDir, actualIMCFile).getAbsolutePath());
  //    assertFileContentIsTheSame(new java.io.File(expectedStorageDir,
  // expectedTSCFile).getAbsolutePath(),
  //        new java.io.File(actualStorageDir, actualTSCFile).getAbsolutePath());
  //    assertFileContentIsTheSame(new java.io.File(expectedStorageDir,
  // expectedNBHFile).getAbsolutePath(),
  //        new java.io.File(actualStorageDir, actualNBHFile).getAbsolutePath());
  //    assertFileContentIsTheSame(new java.io.File(expectedStorageDir,
  // expectedOBFFile).getAbsolutePath(),
  //        new java.io.File(actualStorageDir, actualOBFFile).getAbsolutePath());
  //  }
  //
  //  private void assertFileContentIsTheSame(String expectedBTreeFileName, String
  // actualBTreeFileName) throws IOException {
  //    java.io.File expectedFile = new java.io.File(expectedBTreeFileName);
  //    try (RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r")) {
  //      try (RandomAccessFile fileTwo = new RandomAccessFile(new
  // java.io.File(actualBTreeFileName), "r")) {
  //
  //        Assert.assertEquals(fileOne.length(), fileTwo.length());
  //
  //        byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
  //        byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];
  //
  //        fileOne.seek(OFile.HEADER_SIZE);
  //        fileTwo.seek(OFile.HEADER_SIZE);
  //
  //        int bytesRead = fileOne.read(expectedContent);
  //        while (bytesRead >= 0) {
  //          fileTwo.readFully(actualContent, 0, bytesRead);
  //
  //          Assertions
  //              .assertThat(Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION,
  // ODurablePage.MAX_PAGE_SIZE_BYTES))
  //              .isEqualTo(Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION,
  // ODurablePage.MAX_PAGE_SIZE_BYTES));
  //          expectedContent = new byte[OClusterPage.PAGE_SIZE];
  //          actualContent = new byte[OClusterPage.PAGE_SIZE];
  //          bytesRead = fileOne.read(expectedContent);
  //        }
  //
  //      }
  //    }
  //  }
}
