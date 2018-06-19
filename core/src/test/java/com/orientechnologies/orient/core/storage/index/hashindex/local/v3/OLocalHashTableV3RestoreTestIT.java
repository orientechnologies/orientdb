package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OBinarySerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OClusterPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperationsManager;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OCASDiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OWriteableWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OCreateHashTableOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTablePutOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OHashTableRemoveOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.component.localhashtable.OLocalHashTableOperation;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 5/19/14
 */
public class OLocalHashTableV3RestoreTestIT extends OLocalHashTableV3Base {
  private static final String ENGINE_DIRECTORY = OLocalHashTableV3RestoreTestIT.class.getSimpleName();

  private static final String ORIGINAL_DB_NAME = "originalLocalHashTableTest";
  private static final String RESTORED_DB_NAME = "restoredLocalHashTableTest";

  private static final String HASH_TABLE_NAME = "hashTable";

  private File engineDirectory;

  private OrientDB orientDB;

  private OLogSequenceNumber restoreLSN;

  @Before
  public void before() {
    final String buildDirectory = System.getProperty("buildDirectory", ".");

    engineDirectory = new File(buildDirectory, ENGINE_DIRECTORY);

    OFileUtils.deleteRecursively(engineDirectory);
    orientDB = new OrientDB("plocal:" + engineDirectory, OrientDBConfig.defaultConfig());

    orientDB.create(ORIGINAL_DB_NAME, ODatabaseType.PLOCAL);
    orientDB.create(RESTORED_DB_NAME, ODatabaseType.PLOCAL);

    try (ODatabaseSession databaseDocumentTx = orientDB.open(ORIGINAL_DB_NAME, "admin", "admin")) {
      final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage();

      final OCASDiskWriteAheadLog writeAheadLog = (OCASDiskWriteAheadLog) storage.getWALInstance();
      restoreLSN = storage.getWALInstance().end();
      writeAheadLog.addCutTillLimit(restoreLSN);
      restoreLSN = storage.getWALInstance().end();

      OMurmurHash3HashFunction<Integer> murmurHash3HashFunction = new OMurmurHash3HashFunction<>(OIntegerSerializer.INSTANCE);

      localHashTable = new OLocalHashTableV3<>(HASH_TABLE_NAME, ".imc", ".tsc", ".obf", ".nbh",
          (OAbstractPaginatedStorage) ((ODatabaseInternal) databaseDocumentTx).getStorage());

      localHashTable
          .create(OIntegerSerializer.INSTANCE, OBinarySerializerFactory.getInstance().getObjectSerializer(OType.STRING), null, true,
              null, murmurHash3HashFunction);
    }
  }

  @After
  public void after() {
    orientDB.drop(ORIGINAL_DB_NAME);
    orientDB.drop(RESTORED_DB_NAME);

    orientDB.close();
    OFileUtils.deleteRecursively(engineDirectory);
  }

  @Override
  public void testKeyPut() {
    super.testKeyPut();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  private void assertAtomicOperationEnds() {
    try (ODatabaseSession databaseSession = orientDB.open(ORIGINAL_DB_NAME, "admin", "admin")) {
      Assert.assertNull(
          ((OAbstractPaginatedStorage) ((ODatabaseInternal) databaseSession).getStorage()).getAtomicOperationsManager()
              .getCurrentOperation());
    }
  }

  @Override
  public void testKeyPutRandomUniform() {
    super.testKeyPutRandomUniform();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRandomGaussian() {
    super.testKeyPutRandomGaussian();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDelete() {
    super.testKeyDelete();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomUniform() {
    super.testKeyDeleteRandomUniform();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyDeleteRandomGaussian() {
    super.testKeyDeleteRandomGaussian();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyAddDelete() {
    super.testKeyAddDelete();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  @Override
  public void testKeyPutRemoveNullKey() {
    super.testKeyPutRemoveNullKey();

    assertAtomicOperationEnds();
    assertFileRestoreFromWAL();
  }

  private void assertFileRestoreFromWAL() {
    try {
      System.out.println("Start data restore");
      restoreDataFromWAL();
      System.out.println("Stop data restore");

      final OWOWCache restoredWowCache;
      final Path restoredStorageRoot;

      final OWOWCache originalWowCache;
      final Path originalStorageRoot;

      try (ODatabaseSession session = orientDB.open(RESTORED_DB_NAME, "admin", "admin")) {
        final OLocalPaginatedStorage restoredPaginatedStorage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();

        restoredStorageRoot = restoredPaginatedStorage.getStoragePath();
        restoredWowCache = (OWOWCache) restoredPaginatedStorage.getWriteCache();
      }

      try (ODatabaseSession session = orientDB.open(ORIGINAL_DB_NAME, "admin", "admin")) {
        final OLocalPaginatedStorage originalRestoredStorage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();

        originalStorageRoot = originalRestoredStorage.getStoragePath();
        originalWowCache = (OWOWCache) originalRestoredStorage.getWriteCache();
      }

      final long restoredImcFileId = restoredWowCache.fileIdByName(HASH_TABLE_NAME + ".imc");
      final String restoredImc = restoredWowCache.nativeFileNameById(restoredImcFileId);

      final long restoredTscFileId = restoredWowCache.fileIdByName(HASH_TABLE_NAME + ".tsc");
      final String restoredTsc = restoredWowCache.nativeFileNameById(restoredTscFileId);

      final long restoredNbhFileId = restoredWowCache.fileIdByName(HASH_TABLE_NAME + ".nbh");
      final String restoredNBH = restoredWowCache.nativeFileNameById(restoredNbhFileId);

      final long restoredObfFileId = restoredWowCache.fileIdByName(HASH_TABLE_NAME + ".obf");
      final String restoredOBF = restoredWowCache.nativeFileNameById(restoredObfFileId);

      final long originalImcFileId = originalWowCache.fileIdByName(HASH_TABLE_NAME + ".imc");
      final String originalImc = originalWowCache.nativeFileNameById(originalImcFileId);

      final long originalTscFileId = originalWowCache.fileIdByName(HASH_TABLE_NAME + ".tsc");
      final String originalTsc = originalWowCache.nativeFileNameById(originalTscFileId);

      final long originalNbhFileId = originalWowCache.fileIdByName(HASH_TABLE_NAME + ".nbh");
      final String originalNBH = originalWowCache.nativeFileNameById(originalNbhFileId);

      final long originalObfFileId = originalWowCache.fileIdByName(HASH_TABLE_NAME + ".obf");
      final String originalOBF = originalWowCache.nativeFileNameById(originalObfFileId);

      System.out.println("Start data comparison");
      assertCompareFilesAreTheSame(originalStorageRoot.resolve(originalImc).toFile(),
          restoredStorageRoot.resolve(restoredImc).toFile());

      assertCompareFilesAreTheSame(originalStorageRoot.resolve(originalTsc).toFile(),
          restoredStorageRoot.resolve(restoredTsc).toFile());

      assertCompareFilesAreTheSame(originalStorageRoot.resolve(originalNBH).toFile(),
          restoredStorageRoot.resolve(restoredNBH).toFile());

      assertCompareFilesAreTheSame(originalStorageRoot.resolve(originalOBF).toFile(),
          restoredStorageRoot.resolve(restoredOBF).toFile());
      System.out.println("Stop data comparison");
    } catch (IOException e) {
      throw new IllegalStateException(e);
    }
  }

  private void restoreDataFromWAL() throws IOException {
    OLocalHashTableV3 localHashTable = null;

    final OCASDiskWriteAheadLog originalWriteAheadLog;
    final OLocalPaginatedStorage restoredPaginatedStorage;

    try (ODatabaseSession session = orientDB.open(ORIGINAL_DB_NAME, "admin", "admin")) {
      final OAbstractPaginatedStorage storage = (OAbstractPaginatedStorage) ((ODatabaseInternal) session).getStorage();
      originalWriteAheadLog = (OCASDiskWriteAheadLog) storage.getWALInstance();
    }

    try (ODatabaseSession session = orientDB.open(RESTORED_DB_NAME, "admin", "admin")) {
      restoredPaginatedStorage = (OLocalPaginatedStorage) ((ODatabaseInternal) session).getStorage();
    }

    List<OWriteableWALRecord> walRecordList = originalWriteAheadLog.read(restoreLSN, 100);
    Iterator<OWriteableWALRecord> walRecordIterator = walRecordList.iterator();

    final OAtomicOperationsManager atomicOperationsManager = restoredPaginatedStorage.getAtomicOperationsManager();
    OWriteableWALRecord walRecord;
    while (walRecordIterator.hasNext()) {
      walRecord = walRecordIterator.next();
      if (walRecord instanceof OLocalHashTableOperation) {
        final OLocalHashTableOperation localHashTableOperation = (OLocalHashTableOperation) walRecord;
        if (localHashTableOperation instanceof OCreateHashTableOperation) {
          final OCreateHashTableOperation createHashTableOperation = (OCreateHashTableOperation) localHashTableOperation;

          Assert.assertNull(createHashTableOperation.getEncryptionName());
          Assert.assertNull(createHashTableOperation.getEncryptionOptions());

          localHashTable = new OLocalHashTableV3(createHashTableOperation.getName(),
              createHashTableOperation.getMetadataConfigurationFileExtension(),
              createHashTableOperation.getTreeStateFileExtension(), createHashTableOperation.getBucketFileExtension(),
              createHashTableOperation.getNullBucketFileExtension(), restoredPaginatedStorage);

          final OBinarySerializer keySerializer = OBinarySerializerFactory.getInstance().<Integer>getObjectSerializer(
              createHashTableOperation.getKeySerializerId());

          @SuppressWarnings("unchecked")
          final OMurmurHash3HashFunction murmurHash3HashFunction = new OMurmurHash3HashFunction(keySerializer);

          final OAtomicOperation atomicOperation = atomicOperationsManager.startAtomicOperation((String) null, false);
          //noinspection unchecked
          localHashTable.createRestore(keySerializer,
              OBinarySerializerFactory.getInstance().getObjectSerializer(createHashTableOperation.getValueSerializerId()), null,
              createHashTableOperation.isNullKeyIsSupported(), createHashTableOperation.getKeyTypes(), murmurHash3HashFunction,
              atomicOperation);
          atomicOperationsManager.endAtomicOperation(false, null);
        } else if (walRecord instanceof OHashTablePutOperation) {
          final OHashTablePutOperation putOperation = (OHashTablePutOperation) walRecord;
          final OAtomicOperation atomicOperation = atomicOperationsManager.startAtomicOperation((String) null, false);

          assert localHashTable != null;
          localHashTable.putRestore(putOperation.getKey(), putOperation.getValue(), atomicOperation);
          atomicOperationsManager.endAtomicOperation(false, null);
        } else if (walRecord instanceof OHashTableRemoveOperation) {
          final OHashTableRemoveOperation hashTableRemoveOperation = (OHashTableRemoveOperation) walRecord;
          assert localHashTable != null;

          final OAtomicOperation atomicOperation = atomicOperationsManager.startAtomicOperation((String) null, false);
          localHashTable.removeRestore(hashTableRemoveOperation.getKey(), atomicOperation);
          atomicOperationsManager.endAtomicOperation(false, null);
        } else {
          Assert.fail("Unknown hash table operation " + walRecord.getClass());
        }
      }

      if (!walRecordIterator.hasNext()) {
        walRecordList = originalWriteAheadLog.next(walRecordList.get(walRecordList.size() - 1).getLsn(), 100);
        walRecordIterator = walRecordList.iterator();
      }
    }

  }

  private void assertCompareFilesAreTheSame(File expectedFile, File actualFile) throws IOException {
    try (RandomAccessFile fileOne = new RandomAccessFile(expectedFile, "r")) {
      try (RandomAccessFile fileTwo = new RandomAccessFile(actualFile, "r")) {

        Assert.assertEquals(fileOne.length(), fileTwo.length());

        byte[] expectedContent = new byte[OClusterPage.PAGE_SIZE];
        byte[] actualContent = new byte[OClusterPage.PAGE_SIZE];

        fileOne.seek(OFileClassic.HEADER_SIZE);
        fileTwo.seek(OFileClassic.HEADER_SIZE);

        int bytesRead = fileOne.read(expectedContent);
        while (bytesRead >= 0) {
          fileTwo.readFully(actualContent, 0, bytesRead);

          Assert.assertArrayEquals(Arrays.copyOfRange(expectedContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.PAGE_SIZE),
              Arrays.copyOfRange(actualContent, ODurablePage.NEXT_FREE_POSITION, ODurablePage.PAGE_SIZE));

          expectedContent = new byte[OClusterPage.PAGE_SIZE];
          actualContent = new byte[OClusterPage.PAGE_SIZE];
          bytesRead = fileOne.read(expectedContent);
        }
      }
    }
  }
}
