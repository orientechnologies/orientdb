package com.orientechnologies.orient.core.storage.index.hashindex.local.cache;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WriteAheadLogTest;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.07.13
 */
public class WOWCacheTestIT {
  private int systemOffset = 2 * (OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
  private int pageSize     = systemOffset + 8;

  private static OLocalPaginatedStorage storageLocal;
  private static String                 fileName;

  private static ODiskWriteAheadLog writeAheadLog;

  private static OWOWCache wowCache;
  private OClosableLinkedContainer<Long, OFileClassic> files = new OClosableLinkedContainer<>(1024);

  @BeforeClass
  public static void beforeClass() throws IOException {
    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);
    String buildDirectory = System.getProperty("buildDirectory", ".");

    storageLocal = (OLocalPaginatedStorage) Orient.instance().getRunningEngine("plocal").
        createStorage(buildDirectory + "/WOWCacheTest", null);
    storageLocal.create(new OContextConfiguration());

    fileName = "wowCacheTest.tst";

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, WriteAheadLogTest.TestRecord.class);
  }

  @Before
  public void beforeMethod() throws Exception {
    closeCacheAndDeleteFile();

    initBuffer();
  }

  private static void closeCacheAndDeleteFile() throws IOException {
    String nativeFileName = null;

    if (wowCache != null) {
      long fileId = wowCache.fileIdByName(fileName);
      nativeFileName = wowCache.nativeFileNameById(fileId);

      wowCache.close();
      wowCache = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    storageLocal.delete();

    if (nativeFileName != null) {
      File testFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + nativeFileName);

      if (testFile.exists()) {
        Assert.assertTrue(testFile.delete());
      }
    }

    File nameIdMapFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + "name_id_map.cm");
    if (nameIdMapFile.exists()) {
      Assert.assertTrue(nameIdMapFile.delete());
    }

    nameIdMapFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + "name_id_map_v2.cm");
    if (nameIdMapFile.exists()) {
      Assert.assertTrue(nameIdMapFile.delete());
    }
  }

  @AfterClass
  public static void afterClass() throws IOException {
    closeCacheAndDeleteFile();

    File file = new File(storageLocal.getConfiguration().getDirectory());
    Assert.assertTrue(file.delete());
  }

  private void initBuffer() throws IOException, InterruptedException {
    wowCache = new OWOWCache(pageSize, new OByteBufferPool(pageSize), writeAheadLog, 10, 100, storageLocal, false, files, 1,
        OChecksumMode.StoreAndVerify);
    wowCache.loadRegisteredFiles();
  }

  @Test
  public void testLoadStore() throws IOException {
    Random random = new Random();

    byte[][] pageData = new byte[200][];
    long fileId = wowCache.addFile(fileName);
    final String nativeFileName = wowCache.nativeFileNameById(fileId);

    for (int i = 0; i < pageData.length; i++) {
      byte[] data = new byte[8];
      random.nextBytes(data);

      pageData[i] = data;

      final OCachePointer cachePointer = wowCache.load(fileId, i, 1, true, new OModifiableBoolean(), true)[0];
      cachePointer.acquireExclusiveLock();

      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, i, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataOne = pageData[i];

      OCachePointer cachePointer = wowCache.load(fileId, i, 1, false, new OModifiableBoolean(), true)[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    wowCache.flush();

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataContent = pageData[i];
      assertFile(i, dataContent, new OLogSequenceNumber(0, 0), nativeFileName);
    }
  }

  @Test
  public void testDataUpdate() throws Exception {
    final NavigableMap<Long, byte[]> pageIndexDataMap = new TreeMap<>();
    long fileId = wowCache.addFile(fileName);
    final String nativeFileName = wowCache.nativeFileNameById(fileId);

    Random random = new Random();

    for (int i = 0; i < 600; i++) {
      long pageIndex = random.nextInt(2048);

      byte[] data = new byte[8];
      random.nextBytes(data);

      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, true, new OModifiableBoolean(), true)[0];
      cachePointer.acquireExclusiveLock();
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, pageIndex, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      long pageIndex = entry.getKey();
      byte[] dataOne = entry.getValue();

      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, false, new OModifiableBoolean(), true)[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);

      cachePointer.decrementReadersReferrer();
      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    for (int i = 0; i < 300; i++) {
      long desiredIndex = random.nextInt(2048);

      Long pageIndex = pageIndexDataMap.ceilingKey(desiredIndex);
      if (pageIndex == null)
        pageIndex = pageIndexDataMap.floorKey(desiredIndex);

      byte[] data = new byte[8];
      random.nextBytes(data);
      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, true, new OModifiableBoolean(), true)[0];

      cachePointer.acquireExclusiveLock();
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, pageIndex, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      long pageIndex = entry.getKey();
      byte[] dataOne = entry.getValue();
      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, false, new OModifiableBoolean(), true)[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    wowCache.flush();

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      assertFile(entry.getKey(), entry.getValue(), new OLogSequenceNumber(0, 0), nativeFileName);
    }

  }

  @Test
  public void testFileRestore() throws IOException {
    final long nonDelFileId = wowCache.addFile(fileName);
    final long fileId = wowCache.addFile("removedFile.del");

    final String removedNativeFileName = wowCache.nativeFileNameById(fileId);

    wowCache.deleteFile(fileId);
    File deletedFile = storageLocal.getStoragePath().resolve(removedNativeFileName).toFile();
    Assert.assertTrue(!deletedFile.exists());

    String fileName = wowCache.restoreFileById(fileId);
    Assert.assertEquals(fileName, "removedFile.del");

    fileName = wowCache.restoreFileById(nonDelFileId);
    Assert.assertNull(fileName);
    Assert.assertTrue(deletedFile.exists());

    fileName = wowCache.restoreFileById(1525454L);
    Assert.assertNull(fileName);

    wowCache.deleteFile(fileId);
    Assert.assertTrue(!deletedFile.exists());
  }

  @Test
  public void testFileRestoreAfterClose() throws Exception {
    final long nonDelFileId = wowCache.addFile(fileName);
    final long fileId = wowCache.addFile("removedFile.del");

    final String removedNativeFileName = wowCache.nativeFileNameById(fileId);

    wowCache.deleteFile(fileId);
    File deletedFile = storageLocal.getStoragePath().resolve(removedNativeFileName).toFile();

    Assert.assertTrue(!deletedFile.exists());

    wowCache.close();

    initBuffer();

    String fileName = wowCache.restoreFileById(fileId);
    Assert.assertEquals(fileName, "removedFile.del");

    fileName = wowCache.restoreFileById(nonDelFileId);
    Assert.assertNull(fileName);
    Assert.assertTrue(deletedFile.exists());

    fileName = wowCache.restoreFileById(1525454L);
    Assert.assertNull(fileName);

    wowCache.deleteFile(fileId);
    Assert.assertTrue(!deletedFile.exists());
  }

  @Test
  public void testChecksumFailure() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);

    final long fileId = wowCache.addFile(fileName);
    final OCachePointer cachePointer = wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0];

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getSharedBuffer();
    buffer.position(systemOffset);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    final Path path = storageLocal.getStoragePath().resolve(wowCache.nativeFileNameById(fileId));
    final OFileClassic file = new OFileClassic(path);
    file.open();
    file.writeByte(systemOffset, (byte) 1);
    file.close();

    try {
      wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true);
      Assert.fail();
    } catch (OStorageException e) {
      // ok
    }
  }

  @Test
  public void testMagicFailure() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);

    final long fileId = wowCache.addFile(fileName);
    final OCachePointer cachePointer = wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0];

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getSharedBuffer();
    buffer.position(systemOffset);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    final Path path = storageLocal.getStoragePath().resolve(wowCache.nativeFileNameById(fileId));
    final OFileClassic file = new OFileClassic(path);
    file.open();
    file.writeByte(0, (byte) 1);
    file.close();

    try {
      wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true);
      Assert.fail();
    } catch (OStorageException e) {
      // ok
    }
  }

  @Test
  public void testNoChecksumVerificationIfNotRequested() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);

    final long fileId = wowCache.addFile(fileName);
    final OCachePointer cachePointer = wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0];

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getSharedBuffer();
    buffer.position(systemOffset);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    final Path path = storageLocal.getStoragePath().resolve(wowCache.nativeFileNameById(fileId));
    final OFileClassic file = new OFileClassic(path);
    file.open();
    file.writeByte(systemOffset, (byte) 1);
    file.close();

    wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), false)[0].decrementReadersReferrer();
  }

  @Test
  public void testNoChecksumFailureIfVerificationTurnedOff() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.Off);

    final long fileId = wowCache.addFile(fileName);
    final OCachePointer cachePointer = wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0];

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getSharedBuffer();
    buffer.position(systemOffset);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    final Path path = storageLocal.getStoragePath().resolve(wowCache.nativeFileNameById(fileId));
    final OFileClassic file = new OFileClassic(path);
    file.open();
    file.writeByte(systemOffset, (byte) 1);
    file.close();

    wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0].decrementReadersReferrer();
  }

  @Test
  public void testNoChecksumFailureIfVerificationTurnedOffOnLoad() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.Store);

    final long fileId = wowCache.addFile(fileName);
    final OCachePointer cachePointer = wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0];

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getSharedBuffer();
    buffer.position(systemOffset);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    final Path path = storageLocal.getStoragePath().resolve(wowCache.nativeFileNameById(fileId));
    final OFileClassic file = new OFileClassic(path);
    file.open();
    file.writeByte(systemOffset, (byte) 1);
    file.close();

    wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0].decrementReadersReferrer();
  }

  @Test
  public void testNoChecksumFailureIfNoChecksumProvided() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.Off);

    final long fileId = wowCache.addFile(fileName);
    final OCachePointer cachePointer = wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0];

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getSharedBuffer();
    buffer.position(systemOffset);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    final Path path = storageLocal.getStoragePath().resolve(wowCache.nativeFileNameById(fileId));
    final OFileClassic file = new OFileClassic(path);
    file.open();
    file.writeByte(systemOffset, (byte) 1);
    file.close();

    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);
    wowCache.load(fileId, 0, 1, true, new OModifiableBoolean(), true)[0].decrementReadersReferrer();
  }

  private void assertFile(long pageIndex, byte[] value, OLogSequenceNumber lsn, String fileName) throws IOException {
    OFileClassic fileClassic = new OFileClassic(Paths.get(storageLocal.getConfiguration().getDirectory(), fileName));
    fileClassic.open();
    byte[] content = new byte[8 + systemOffset];
    fileClassic.read(pageIndex * (8 + systemOffset), content, 8 + systemOffset);

    Assert.assertArrayEquals(Arrays.copyOfRange(content, systemOffset, 8 + systemOffset), value);

    long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, 0);
    Assert.assertEquals(magicNumber, OWOWCache.MAGIC_NUMBER_WITH_CHECKSUM);

    int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);
    long position = OLongSerializer.INSTANCE
        .deserializeNative(content, OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE);

    OLogSequenceNumber readLsn = new OLogSequenceNumber(segment, position);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

}
