package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OReadCache;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientechnologies.com)
 * @since 5/19/14
 */
@Test
public class OLocalHashTableWAL extends OLocalHashTableTest {

  static {
    OGlobalConfiguration.FILE_LOCK.setValue(false);
    OGlobalConfiguration.WAL_FUZZY_CHECKPOINT_INTERVAL.setValue(100000000);
  }

  private String              buildDirectory;

  private String              actualStorageDir;
  private String              expectedStorageDir;

  private ODatabaseDocumentTx expectedDatabaseDocumentTx;

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
    OGlobalConfiguration.INDEX_TX_MODE.setValue("FULL");
    OGlobalConfiguration.WAL_MAX_SIZE.setValue(200 * 1024);

    buildDirectory = System.getProperty("buildDirectory", ".");

    buildDirectory += "/" + this.getClass().getSimpleName();

    final File buildDir = new File(buildDirectory);
    if (buildDir.exists())
      buildDir.delete();

    buildDir.mkdir();

    final String actualStorageName = this.getClass().getSimpleName() + "Actual";
    databaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + File.separator + actualStorageName);
    if (databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
    }

    databaseDocumentTx.create();

    final String expectedStorageName = this.getClass().getSimpleName() + "Expected";
    expectedDatabaseDocumentTx = new ODatabaseDocumentTx("plocal:" + buildDirectory + File.separator + expectedStorageName);
    if (expectedDatabaseDocumentTx.exists()) {
      expectedDatabaseDocumentTx.open("admin", "admin");
      expectedDatabaseDocumentTx.drop();
    }

    expectedDatabaseDocumentTx.create();

    actualStorageDir = ((OLocalPaginatedStorage) databaseDocumentTx.getStorage()).getStoragePath();
    expectedStorageDir = ((OLocalPaginatedStorage) expectedDatabaseDocumentTx.getStorage()).getStoragePath();

    createActualHashTable();
  }

  @AfterMethod
  @Override
  public void afterMethod() throws IOException {
    if (databaseDocumentTx.isClosed())
      databaseDocumentTx.open("admin", "admin");

    databaseDocumentTx.drop();

    if (expectedDatabaseDocumentTx.isClosed())
      expectedDatabaseDocumentTx.open("admin", "admin");

    expectedDatabaseDocumentTx.drop();

    Assert.assertTrue(new File(buildDirectory).delete());
  }

  private void createActualHashTable() throws IOException {
    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<Integer>();
    murmurHash3HashFunction.setValueSerializer(OIntegerSerializer.INSTANCE);

    localHashTable = new OLocalHashTable<Integer, String>("actualLocalHashTable", ".imc", ".tsc", ".obf", ".nbh",
        murmurHash3HashFunction, true, (OAbstractPaginatedStorage) databaseDocumentTx.getStorage());
    localHashTable.create(OIntegerSerializer.INSTANCE,
        OBinarySerializerFactory.getInstance().<String> getObjectSerializer(OType.STRING), null, true);
  }

  @Override
  public void testKeyPut() throws IOException {
    super.testKeyPut();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomUniform() throws IOException {
    super.testKeyPutRandomUniform();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomGaussian() throws IOException {
    super.testKeyPutRandomGaussian();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDelete() throws IOException {
    super.testKeyDelete();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomUniform() throws IOException {
    super.testKeyDeleteRandomUniform();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomGaussian() throws IOException {
    super.testKeyDeleteRandomGaussian();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyAddDelete() throws IOException {
    super.testKeyAddDelete();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRemoveNullKey() throws IOException {
    super.testKeyPutRemoveNullKey();

    Assert.assertNull(((OAbstractPaginatedStorage) databaseDocumentTx.getStorage()).getAtomicOperationsManager()
        .getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  private void assertFileRestoreFromWAL() throws IOException {
    localHashTable.close();

    System.out.println("Start data restore");
    restoreDataFromWAL();
    System.out.println("Stop data restore");

    databaseDocumentTx.close();
    expectedDatabaseDocumentTx.close();

    System.out.println("Start data comparison");
    assertFileContentIsTheSame("expectedLocalHashTable", "actualLocalHashTable");
    System.out.println("Stop data comparison");
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
    log.close();

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
              || restoreRecord instanceof OCheckpointEndRecord)
            continue;

          if (restoreRecord instanceof OUpdatePageRecord) {
            final OUpdatePageRecord updatePageRecord = (OUpdatePageRecord) restoreRecord;

            final long fileId = updatePageRecord.getFileId();
            final long pageIndex = updatePageRecord.getPageIndex();

            if (!expectedWriteCache.isOpen(fileId))
              expectedReadCache.openFile(fileId, expectedWriteCache);

            OCacheEntry cacheEntry = expectedReadCache.load(fileId, pageIndex, true, expectedWriteCache);
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
            String fileName = fileCreatedCreatedRecord.getFileName().replace("actualLocalHashTable", "expectedLocalHashTable");

            if (expectedWriteCache.exists(fileName))
              expectedReadCache.openFile(fileName, fileCreatedCreatedRecord.getFileId(), expectedWriteCache);
            else
              expectedReadCache.addFile(fileName, expectedWriteCache);
          }
        }

        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof OFileCreatedWALRecord
            || walRecord instanceof ONonTxOperationPerformedWALRecord || walRecord instanceof OFullCheckpointStartRecord
            || walRecord instanceof OCheckpointEndRecord);
      }

    }

    return atomicChangeIsProcessed;
  }

  private void assertFileContentIsTheSame(String expectedLocalHashTable, String actualLocalHashTable) throws IOException {
    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedLocalHashTable + ".imc"), new File(actualStorageDir,
        actualLocalHashTable + ".imc"));
    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedLocalHashTable + ".tsc"), new File(actualStorageDir,
        actualLocalHashTable + ".tsc"));
    assertCompareFilesAreTheSame(new File(expectedStorageDir, expectedLocalHashTable + ".nbh"), new File(actualStorageDir,
        actualLocalHashTable + ".nbh"));

    File expectedStorageDirFile = new File(expectedStorageDir);

    File[] expectedDataFiles = expectedStorageDirFile.listFiles(new FilenameFilter() {
      @Override
      public boolean accept(File dir, String name) {
        return name.endsWith(".obf");
      }
    });

    for (File expectedDataFile : expectedDataFiles) {
      String fileName = expectedDataFile.getName();
      File actualDataFile = new File(actualStorageDir, "actualLocalHashTable" + fileName.charAt(fileName.length() - 5) + ".obf");
      assertCompareFilesAreTheSame(expectedDataFile, actualDataFile);
    }
  }

  private void assertCompareFilesAreTheSame(File expectedFile, File actualFile) throws IOException {
    RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r");
    RandomAccessFile fileTwo = new RandomAccessFile(actualFile, "r");

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
