package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCacheEntry;
import com.orientechnologies.orient.core.index.hashindex.local.cache.OCachePointer;
import com.orientechnologies.orient.core.index.hashindex.local.cache.ODiskCache;
import com.orientechnologies.orient.core.memory.OMemoryWatchDog;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.fs.OAbstractFile;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocalAbstract;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.testng.Assert;
import org.testng.annotations.*;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 5/19/14
 */
@Test(enabled = false)
public class OLocalHashTableWAL extends OLocalHashTableTest {
  private String              buildDirectory;

  private String              actualStorageDir;
  private String              expectedStorageDir;

  private ODatabaseDocumentTx expectedDatabaseDocumentTx;

  @BeforeClass(enabled = false)
  @Override
  public void beforeClass() {
  }

  @AfterClass(enabled = false)
  @Override
  public void afterClass() {
  }

  @BeforeMethod(enabled = false)
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

    actualStorageDir = ((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getStoragePath();
    expectedStorageDir = ((OStorageLocalAbstract) expectedDatabaseDocumentTx.getStorage()).getStoragePath();

    createActualHashTable();
  }

  @AfterMethod(enabled = false)
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

    localHashTable = new OLocalHashTable<Integer, String>(".imc", ".tsc", ".obf", ".nbh", murmurHash3HashFunction, true);
    localHashTable.create("actualLocalHashTable", OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance()
        .<String> getObjectSerializer(OType.STRING), null, (OStorageLocalAbstract) databaseDocumentTx.getStorage(), true);
  }

  @Override
  public void testKeyPut() throws IOException {
    super.testKeyPut();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomUniform() throws IOException {
    super.testKeyPutRandomUniform();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomGaussian() throws IOException {
    super.testKeyPutRandomGaussian();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDelete() throws IOException {
    super.testKeyDelete();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomUniform() throws IOException {
    super.testKeyDeleteRandomUniform();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomGaussian() throws IOException {
    super.testKeyDeleteRandomGaussian();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyAddDelete() throws IOException {
    super.testKeyAddDelete();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRemoveNullKey() throws IOException {
    super.testKeyPutRemoveNullKey();

    Assert.assertNull(((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getAtomicOperationsManager().getCurrentOperation());

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
    OWriteAheadLog log = ((OStorageLocalAbstract) databaseDocumentTx.getStorage()).getWALInstance();

    OLogSequenceNumber lsn = log.begin();

    List<OWALRecord> atomicUnit = new ArrayList<OWALRecord>();
    List<OWALRecord> batch = new ArrayList<OWALRecord>();

    final AtomicBoolean lowMemory = new AtomicBoolean(false);

    OMemoryWatchDog.Listener watchdogListener = new OMemoryWatchDog.Listener() {
      @Override
      public void lowMemory(long iFreeMemory, long iFreeMemoryPercentage) {
        lowMemory.set(true);
      }
    };

    watchdogListener = Orient.instance().getMemoryWatchDog().addListener(watchdogListener);

    boolean atomicChangeIsProcessed = false;
    while (lsn != null) {
      OWALRecord walRecord = log.read(lsn);
      batch.add(walRecord);

      if (lowMemory.get()) {
        System.out.println("Heap memory is low, apply batch");
        atomicChangeIsProcessed = restoreDataFromBatch(atomicChangeIsProcessed, atomicUnit, batch);
        batch = new ArrayList<OWALRecord>();

        lowMemory.set(false);
      }

      lsn = log.next(lsn);
    }

    if (batch.size() > 0) {
      System.out.println("Apply batch the last batch.");
      restoreDataFromBatch(atomicChangeIsProcessed, atomicUnit, batch);
      batch = null;
    }

    Assert.assertTrue(atomicUnit.isEmpty());
    log.close();

    Orient.instance().getMemoryWatchDog().removeListener(watchdogListener);

    final ODiskCache expectedDiskCache = ((OStorageLocalAbstract) expectedDatabaseDocumentTx.getStorage()).getDiskCache();
    expectedDiskCache.flushBuffer();
  }

  private boolean restoreDataFromBatch(boolean atomicChangeIsProcessed, List<OWALRecord> atomicUnit, List<OWALRecord> records)
      throws IOException {

    final ODiskCache expectedDiskCache = ((OStorageLocalAbstract) expectedDatabaseDocumentTx.getStorage()).getDiskCache();
    for (OWALRecord walRecord : records) {
      atomicUnit.add(walRecord);

      if (!atomicChangeIsProcessed) {
        Assert.assertTrue(walRecord instanceof OAtomicUnitStartRecord);
        atomicChangeIsProcessed = true;
      } else if (walRecord instanceof OAtomicUnitEndRecord) {
        atomicChangeIsProcessed = false;

        for (OWALRecord restoreRecord : atomicUnit) {
          if (restoreRecord instanceof OAtomicUnitStartRecord || restoreRecord instanceof OAtomicUnitEndRecord)
            continue;

          if (restoreRecord instanceof OUpdatePageRecord) {
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
          } else if (restoreRecord instanceof OFileCreatedCreatedWALRecord) {
            final OFileCreatedCreatedWALRecord fileCreatedCreatedRecord = (OFileCreatedCreatedWALRecord) restoreRecord;
            expectedDiskCache.openFile(
                fileCreatedCreatedRecord.getFileName().replace("actualLocalHashTable", "expectedLocalHashTable"),
                fileCreatedCreatedRecord.getFileId());
          }
        }

        atomicUnit.clear();
      } else {
        Assert.assertTrue(walRecord instanceof OUpdatePageRecord || walRecord instanceof OFileCreatedCreatedWALRecord);
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