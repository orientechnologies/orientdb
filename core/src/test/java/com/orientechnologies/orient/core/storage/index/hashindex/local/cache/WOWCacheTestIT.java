package com.orientechnologies.orient.core.storage.index.hashindex.local.cache;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.ODirectMemoryAllocator.Intention;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLogNoOP;
import com.orientechnologies.orient.core.storage.fs.AsyncFile;
import com.orientechnologies.orient.core.storage.fs.OFile;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.CASDiskWriteAheadLog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com)
 * @since 26.07.13
 */
public class WOWCacheTestIT {
  private static final int pageSize = ODurablePage.NEXT_FREE_POSITION + 8;

  private static String fileName;

  private static CASDiskWriteAheadLog writeAheadLog;
  private static final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
  private static Path storagePath;
  private static OWOWCache wowCache;
  private static String storageName;

  private final OClosableLinkedContainer<Long, OFile> files = new OClosableLinkedContainer<>(1024);

  @BeforeClass
  public static void beforeClass() {
    OGlobalConfiguration.STORAGE_EXCLUSIVE_FILE_ACCESS.setValue(Boolean.FALSE);
    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);
    String buildDirectory = System.getProperty("buildDirectory", ".");

    fileName = "wowCacheTest.tst";
    storageName = "WOWCacheTest";
    storagePath = Paths.get(buildDirectory).resolve(storageName);

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
  }

  @Before
  public void beforeMethod() throws Exception {
    deleteCacheAndDeleteFile();

    initBuffer();
  }

  private static void deleteCacheAndDeleteFile() throws IOException {
    String nativeFileName = null;

    if (wowCache != null) {
      long fileId = wowCache.fileIdByName(fileName);
      nativeFileName = wowCache.nativeFileNameById(fileId);

      wowCache.delete();
      wowCache = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    if (nativeFileName != null) {
      java.io.File testFile = storagePath.resolve(nativeFileName).toFile();

      if (testFile.exists()) {
        Assert.assertTrue(testFile.delete());
      }
    }

    java.io.File nameIdMapFile = storagePath.resolve("name_id_map.cm").toFile();
    if (nameIdMapFile.exists()) {
      Assert.assertTrue(nameIdMapFile.delete());
    }

    nameIdMapFile = storagePath.resolve("name_id_map_v2.cm").toFile();
    if (nameIdMapFile.exists()) {
      Assert.assertTrue(nameIdMapFile.delete());
    }
  }

  @AfterClass
  public static void afterClass() throws IOException {
    deleteCacheAndDeleteFile();

    Files.deleteIfExists(storagePath);
    bufferPool.clear();

    OGlobalConfiguration.STORAGE_EXCLUSIVE_FILE_ACCESS.setValue(Boolean.TRUE);
    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.TRUE);
  }

  private void initBuffer() throws IOException, InterruptedException {
    Files.createDirectories(storagePath);

    writeAheadLog =
        new CASDiskWriteAheadLog(
            storageName,
            storagePath,
            storagePath,
            12_000,
            128,
            null,
            null,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            25,
            true,
            Locale.US,
            -1,
            1000,
            false,
            false,
            true,
            10);
    wowCache =
        new OWOWCache(
            pageSize,
            bufferPool,
            writeAheadLog,
            new DoubleWriteLogNoOP(),
            10,
            10,
            100,
            storagePath,
            storageName,
            OStringSerializer.INSTANCE,
            files,
            1,
            OChecksumMode.StoreAndVerify,
            null,
            null,
            false);

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

      final int pageIndex = wowCache.allocateNewPage(fileId);
      Assert.assertEquals(i, pageIndex);
      final OCachePointer cachePointer = wowCache.load(fileId, i, new OModifiableBoolean(), false);
      cachePointer.acquireExclusiveLock();

      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, i, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataOne = pageData[i];

      OCachePointer cachePointer = wowCache.load(fileId, i, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;
      buffer.position(ODurablePage.NEXT_FREE_POSITION);
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
  public void testLoadStoreEncrypted() throws Exception {
    deleteCacheAndDeleteFile();

    final String aesKeyEncoded = "T1JJRU5UREJfSVNfQ09PTA==";
    final byte[] aesKey = Base64.getDecoder().decode(aesKeyEncoded);
    final byte[] iv = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    Files.createDirectories(storagePath);

    writeAheadLog =
        new CASDiskWriteAheadLog(
            storageName,
            storagePath,
            storagePath,
            12_000,
            128,
            aesKey,
            iv,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            25,
            true,
            Locale.US,
            -1,
            1000,
            false,
            false,
            true,
            10);
    wowCache =
        new OWOWCache(
            pageSize,
            bufferPool,
            writeAheadLog,
            new DoubleWriteLogNoOP(),
            10,
            10,
            100,
            storagePath,
            storageName,
            OStringSerializer.INSTANCE,
            files,
            1,
            OChecksumMode.StoreAndVerify,
            iv,
            aesKey,
            false);

    wowCache.loadRegisteredFiles();

    Random random = new Random();

    byte[][] pageData = new byte[200][];
    long fileId = wowCache.addFile(fileName);
    final String nativeFileName = wowCache.nativeFileNameById(fileId);

    for (int i = 0; i < pageData.length; i++) {
      byte[] data = new byte[8];
      random.nextBytes(data);

      pageData[i] = data;

      final int pageIndex = wowCache.allocateNewPage(fileId);
      Assert.assertEquals(i, pageIndex);
      final OCachePointer cachePointer = wowCache.load(fileId, i, new OModifiableBoolean(), false);
      cachePointer.acquireExclusiveLock();

      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, i, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataOne = pageData[i];

      OCachePointer cachePointer = wowCache.load(fileId, i, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    wowCache.flush();

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataOne = pageData[i];

      OCachePointer cachePointer = wowCache.load(fileId, i, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataContent = pageData[i];
      assertFileEncrypted(
          wowCache.internalFileId(fileId),
          i,
          dataContent,
          new OLogSequenceNumber(0, 0),
          nativeFileName,
          aesKey,
          iv);
    }
  }

  @Test
  public void testDataUpdate() throws Exception {
    final NavigableMap<Long, byte[]> pageIndexDataMap = new TreeMap<>();
    long fileId = wowCache.addFile(fileName);
    final String nativeFileName = wowCache.nativeFileNameById(fileId);

    Random random = new Random();

    for (int i = 0; i < 2048; i++) {
      wowCache.allocateNewPage(fileId);

      final OPointer pointer = bufferPool.acquireDirect(true, Intention.TEST);
      final OCachePointer cachePointer = new OCachePointer(pointer, bufferPool, fileId, i);

      cachePointer.incrementReadersReferrer();
      wowCache.store(fileId, i, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (int i = 0; i < 600; i++) {
      long pageIndex = random.nextInt(2048);

      byte[] data = new byte[8];
      random.nextBytes(data);

      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer =
          wowCache.load(fileId, pageIndex, new OModifiableBoolean(), false);
      cachePointer.acquireExclusiveLock();
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      Assert.assertEquals(
          new OLogSequenceNumber(0, 0), ODurablePage.getLogSequenceNumberFromPage(buffer));

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, pageIndex, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      long pageIndex = entry.getKey();
      byte[] dataOne = entry.getValue();

      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.get(dataTwo);

      cachePointer.decrementReadersReferrer();
      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    for (int i = 0; i < 300; i++) {
      long desiredIndex = random.nextInt(2048);

      Long pageIndex = pageIndexDataMap.ceilingKey(desiredIndex);
      if (pageIndex == null) pageIndex = pageIndexDataMap.floorKey(desiredIndex);

      byte[] data = new byte[8];
      random.nextBytes(data);
      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer =
          wowCache.load(fileId, pageIndex, new OModifiableBoolean(), true);

      cachePointer.acquireExclusiveLock();
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.put(data);
      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, pageIndex, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      long pageIndex = entry.getKey();
      byte[] dataOne = entry.getValue();
      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
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
  public void testDataUpdateEncrypted() throws Exception {
    deleteCacheAndDeleteFile();

    final String aesKeyEncoded = "T1JJRU5UREJfSVNfQ09PTA==";
    final byte[] aesKey = Base64.getDecoder().decode(aesKeyEncoded);
    final byte[] iv = new byte[] {1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16};

    Files.createDirectories(storagePath);

    writeAheadLog =
        new CASDiskWriteAheadLog(
            storageName,
            storagePath,
            storagePath,
            12_000,
            128,
            aesKey,
            iv,
            Integer.MAX_VALUE,
            Integer.MAX_VALUE,
            25,
            true,
            Locale.US,
            -1,
            1000,
            false,
            false,
            true,
            10);
    wowCache =
        new OWOWCache(
            pageSize,
            bufferPool,
            writeAheadLog,
            new DoubleWriteLogNoOP(),
            10,
            10,
            100,
            storagePath,
            storageName,
            OStringSerializer.INSTANCE,
            files,
            1,
            OChecksumMode.StoreAndVerify,
            iv,
            aesKey,
            false);

    wowCache.loadRegisteredFiles();

    final NavigableMap<Long, byte[]> pageIndexDataMap = new TreeMap<>();
    long fileId = wowCache.addFile(fileName);
    final String nativeFileName = wowCache.nativeFileNameById(fileId);

    final long seed = System.nanoTime();
    System.out.println("testDataUpdateEncrypted : seed " + seed);
    Random random = new Random(seed);

    for (int i = 0; i < 2048; i++) {
      final long position = wowCache.allocateNewPage(fileId);
      Assert.assertEquals(i, position);
    }

    for (int i = 0; i < 600; i++) {
      long pageIndex = random.nextInt(2048);

      byte[] data = new byte[8];
      random.nextBytes(data);

      boolean verifyChecksums = pageIndexDataMap.containsKey(pageIndex);
      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer =
          wowCache.load(fileId, pageIndex, new OModifiableBoolean(), verifyChecksums);
      ByteBuffer bufferDuplicate = cachePointer.getBufferDuplicate();
      assert bufferDuplicate != null;

      Assert.assertEquals(
          new OLogSequenceNumber(0, 0), ODurablePage.getLogSequenceNumberFromPage(bufferDuplicate));

      cachePointer.acquireExclusiveLock();
      ByteBuffer buffer = cachePointer.getBufferDuplicate();

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.put(data);

      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, pageIndex, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      long pageIndex = entry.getKey();
      byte[] dataOne = entry.getValue();

      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.get(dataTwo);

      cachePointer.decrementReadersReferrer();
      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    for (int i = 0; i < 300; i++) {
      long desiredIndex = random.nextInt(2048);

      Long pageIndex = pageIndexDataMap.ceilingKey(desiredIndex);
      if (pageIndex == null) pageIndex = pageIndexDataMap.floorKey(desiredIndex);

      byte[] data = new byte[8];
      random.nextBytes(data);
      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer =
          wowCache.load(fileId, pageIndex, new OModifiableBoolean(), true);

      cachePointer.acquireExclusiveLock();
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.put(data);

      cachePointer.releaseExclusiveLock();

      wowCache.store(fileId, pageIndex, cachePointer);
      cachePointer.decrementReadersReferrer();
    }

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      long pageIndex = entry.getKey();
      byte[] dataOne = entry.getValue();
      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, new OModifiableBoolean(), true);
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getBufferDuplicate();
      assert buffer != null;

      buffer.position(ODurablePage.NEXT_FREE_POSITION);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertArrayEquals(dataTwo, dataOne);
    }

    wowCache.flush();

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      assertFileEncrypted(
          wowCache.internalFileId(fileId),
          (int) entry.getKey().longValue(),
          entry.getValue(),
          new OLogSequenceNumber(0, 0),
          nativeFileName,
          aesKey,
          iv);
    }
  }

  @Test
  public void testFileRestore() throws IOException {
    final long nonDelFileId = wowCache.addFile(fileName);
    final long fileId = wowCache.addFile("removedFile.del");

    final String removedNativeFileName = wowCache.nativeFileNameById(fileId);
    assert removedNativeFileName != null;

    wowCache.deleteFile(fileId);
    java.io.File deletedFile = storagePath.resolve(removedNativeFileName).toFile();
    Assert.assertFalse(deletedFile.exists());

    String fileName = wowCache.restoreFileById(fileId);
    Assert.assertEquals(fileName, "removedFile.del");

    fileName = wowCache.restoreFileById(nonDelFileId);
    Assert.assertNull(fileName);
    Assert.assertTrue(deletedFile.exists());

    fileName = wowCache.restoreFileById(1525454L);
    Assert.assertNull(fileName);

    wowCache.deleteFile(fileId);
    Assert.assertFalse(deletedFile.exists());
  }

  @Test
  public void testFileRestoreAfterClose() throws Exception {
    final long nonDelFileId = wowCache.addFile(fileName);
    final long fileId = wowCache.addFile("removedFile.del");

    final String removedNativeFileName = wowCache.nativeFileNameById(fileId);
    assert removedNativeFileName != null;

    wowCache.deleteFile(fileId);
    java.io.File deletedFile = storagePath.resolve(removedNativeFileName).toFile();

    Assert.assertFalse(deletedFile.exists());

    wowCache.close();
    writeAheadLog.close();

    initBuffer();

    String fileName = wowCache.restoreFileById(fileId);
    Assert.assertEquals(fileName, "removedFile.del");

    fileName = wowCache.restoreFileById(nonDelFileId);
    Assert.assertNull(fileName);
    Assert.assertTrue(deletedFile.exists());

    fileName = wowCache.restoreFileById(1525454L);
    Assert.assertNull(fileName);

    wowCache.deleteFile(fileId);
    Assert.assertFalse(deletedFile.exists());
  }

  @Test
  public void testChecksumFailure() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);

    final long fileId = wowCache.addFile(fileName);
    Assert.assertEquals(0, wowCache.allocateNewPage(fileId));
    final OCachePointer cachePointer = wowCache.load(fileId, 0, new OModifiableBoolean(), false);

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;

    buffer.position(ODurablePage.NEXT_FREE_POSITION);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;

    final Path path = storagePath.resolve(fileName);
    final OFile file = new AsyncFile(path, pageSize);
    file.open();
    file.write(
        ODurablePage.NEXT_FREE_POSITION,
        ByteBuffer.wrap(new byte[] {1}).order(ByteOrder.nativeOrder()));
    file.close();

    try {
      wowCache.load(fileId, 0, new OModifiableBoolean(), true);
      Assert.fail();
    } catch (OStorageException e) {
      // ok
    }
  }

  @Test
  public void testMagicFailure() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);

    final long fileId = wowCache.addFile(fileName);
    Assert.assertEquals(0, wowCache.allocateNewPage(fileId));
    final OCachePointer cachePointer = wowCache.load(fileId, 0, new OModifiableBoolean(), false);

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;

    buffer.position(ODurablePage.NEXT_FREE_POSITION);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;

    final Path path = storagePath.resolve(fileName);
    final OFile file = new AsyncFile(path, pageSize);
    file.open();
    file.write(0, ByteBuffer.wrap(new byte[] {1}).order(ByteOrder.nativeOrder()));
    file.close();

    try {
      wowCache.load(fileId, 0, new OModifiableBoolean(), true);
      Assert.fail();
    } catch (OStorageException e) {
      // ok
    }
  }

  @Test
  public void testNoChecksumVerificationIfNotRequested() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);

    final long fileId = wowCache.addFile(fileName);
    Assert.assertEquals(0, wowCache.allocateNewPage(fileId));
    final OCachePointer cachePointer = wowCache.load(fileId, 0, new OModifiableBoolean(), false);

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;

    buffer.position(ODurablePage.NEXT_FREE_POSITION);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;

    final Path path = storagePath.resolve(fileName);
    final OFile file = new AsyncFile(path, pageSize);
    file.open();
    file.write(
        ODurablePage.NEXT_FREE_POSITION,
        ByteBuffer.wrap(new byte[] {1}).order(ByteOrder.nativeOrder()));
    file.close();

    wowCache.load(fileId, 0, new OModifiableBoolean(), false).decrementReadersReferrer();
  }

  @Test
  public void testNoChecksumFailureIfVerificationTurnedOff() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.Off);

    final long fileId = wowCache.addFile(fileName);
    Assert.assertEquals(0, wowCache.allocateNewPage(fileId));
    final OCachePointer cachePointer = wowCache.load(fileId, 0, new OModifiableBoolean(), true);

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;

    buffer.position(ODurablePage.NEXT_FREE_POSITION);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;

    final Path path = storagePath.resolve(fileName);
    final OFile file = new AsyncFile(path, pageSize);
    file.open();
    file.write(
        ODurablePage.NEXT_FREE_POSITION,
        ByteBuffer.wrap(new byte[] {1}).order(ByteOrder.nativeOrder()));
    file.close();

    wowCache.load(fileId, 0, new OModifiableBoolean(), true).decrementReadersReferrer();
  }

  @Test
  public void testNoChecksumFailureIfVerificationTurnedOffOnLoad() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.Store);

    final long fileId = wowCache.addFile(fileName);
    Assert.assertEquals(0, wowCache.allocateNewPage(fileId));
    final OCachePointer cachePointer = wowCache.load(fileId, 0, new OModifiableBoolean(), true);

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;

    buffer.position(ODurablePage.NEXT_FREE_POSITION);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;

    final Path path = storagePath.resolve(fileName);
    final OFile file = new AsyncFile(path, pageSize);
    file.open();
    file.write(
        ODurablePage.NEXT_FREE_POSITION,
        ByteBuffer.wrap(new byte[] {1}).order(ByteOrder.nativeOrder()));
    file.close();

    wowCache.load(fileId, 0, new OModifiableBoolean(), true).decrementReadersReferrer();
  }

  @Test
  public void testNoChecksumFailureIfNoChecksumProvided() throws IOException {
    wowCache.setChecksumMode(OChecksumMode.Off);

    final long fileId = wowCache.addFile(fileName);
    Assert.assertEquals(0, wowCache.allocateNewPage(fileId));
    final OCachePointer cachePointer = wowCache.load(fileId, 0, new OModifiableBoolean(), true);

    cachePointer.acquireExclusiveLock();
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;

    buffer.position(ODurablePage.NEXT_FREE_POSITION);
    buffer.put(new byte[buffer.remaining()]);
    cachePointer.releaseExclusiveLock();

    wowCache.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    wowCache.flush();

    String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;

    final Path path = storagePath.resolve(fileName);
    final OFile file = new AsyncFile(path, pageSize);
    file.open();
    file.write(
        ODurablePage.NEXT_FREE_POSITION,
        ByteBuffer.wrap(new byte[] {1}).order(ByteOrder.nativeOrder()));
    file.close();

    wowCache.setChecksumMode(OChecksumMode.StoreAndThrow);
    wowCache.load(fileId, 0, new OModifiableBoolean(), true).decrementReadersReferrer();
  }

  private static void assertFile(
      long pageIndex, byte[] value, OLogSequenceNumber lsn, String fileName) throws IOException {
    OFile fileClassic = new AsyncFile(storagePath.resolve(fileName), pageSize);
    fileClassic.open();
    byte[] content = new byte[8 + ODurablePage.NEXT_FREE_POSITION];
    fileClassic.read(
        pageIndex * (8 + ODurablePage.NEXT_FREE_POSITION),
        ByteBuffer.wrap(content).order(ByteOrder.nativeOrder()),
        true);

    Assert.assertArrayEquals(
        Arrays.copyOfRange(
            content, ODurablePage.NEXT_FREE_POSITION, 8 + ODurablePage.NEXT_FREE_POSITION),
        value);

    long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, 0);
    Assert.assertEquals(magicNumber, OWOWCache.MAGIC_NUMBER_WITH_CHECKSUM);

    long segment =
        OLongSerializer.INSTANCE.deserializeNative(content, ODurablePage.WAL_SEGMENT_OFFSET);
    int position =
        OIntegerSerializer.INSTANCE.deserializeNative(content, ODurablePage.WAL_POSITION_OFFSET);

    OLogSequenceNumber readLsn = new OLogSequenceNumber(segment, position);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

  private static void assertFileEncrypted(
      int fileId,
      int pageIndex,
      byte[] value,
      OLogSequenceNumber lsn,
      String fileName,
      final byte[] aesKey,
      final byte[] iv)
      throws Exception {
    OFile fileClassic = new AsyncFile(storagePath.resolve(fileName), pageSize);
    fileClassic.open();
    byte[] content = new byte[8 + ODurablePage.NEXT_FREE_POSITION];
    fileClassic.read(
        (long) pageIndex * (8 + ODurablePage.NEXT_FREE_POSITION),
        ByteBuffer.wrap(content).order(ByteOrder.nativeOrder()),
        true);

    final Cipher cipher = Cipher.getInstance("AES/CTR/NoPadding");

    final SecretKey secretKey = new SecretKeySpec(aesKey, "AES");

    final long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, 0);
    final long updateCounter = magicNumber >>> 8;

    final byte[] updatedIv = new byte[iv.length];

    for (int i = 0; i < OIntegerSerializer.INT_SIZE; i++) {
      updatedIv[i] = (byte) (iv[i] ^ ((pageIndex >>> i) & 0xFF));
    }

    for (int i = 0; i < OIntegerSerializer.INT_SIZE; i++) {
      updatedIv[i + OIntegerSerializer.INT_SIZE] =
          (byte) (iv[i + OIntegerSerializer.INT_SIZE] ^ ((fileId >>> i) & 0xFF));
    }

    for (int i = 0; i < OLongSerializer.LONG_SIZE - 1; i++) {
      updatedIv[i + 2 * OIntegerSerializer.INT_SIZE] =
          (byte) (iv[i + 2 * OIntegerSerializer.INT_SIZE] ^ ((updateCounter >>> i) & 0xFF));
    }

    updatedIv[updatedIv.length - 1] = iv[iv.length - 1];

    cipher.init(Cipher.DECRYPT_MODE, secretKey, new IvParameterSpec(updatedIv));
    System.arraycopy(
        cipher.doFinal(
            content, OWOWCache.CHECKSUM_OFFSET, content.length - OWOWCache.CHECKSUM_OFFSET),
        0,
        content,
        OWOWCache.CHECKSUM_OFFSET,
        content.length - OWOWCache.CHECKSUM_OFFSET);

    Assert.assertArrayEquals(
        Arrays.copyOfRange(
            content, ODurablePage.NEXT_FREE_POSITION, 8 + ODurablePage.NEXT_FREE_POSITION),
        value);

    Assert.assertEquals(magicNumber & 0xFF, OWOWCache.MAGIC_NUMBER_WITH_CHECKSUM_ENCRYPTED);

    OLogSequenceNumber readLsn = ODurablePage.getLogSequenceNumber(0, content);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[] data;

    @SuppressWarnings("unused")
    public TestRecord() {}

    @SuppressWarnings("unused")
    public TestRecord(byte[] data) {
      this.data = data;
    }

    @Override
    public int toStream(byte[] content, int offset) {
      OIntegerSerializer.INSTANCE.serializeNative(data.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(data, 0, content, offset, data.length);
      offset += data.length;

      return offset;
    }

    @Override
    public void toStream(ByteBuffer buffer) {
      buffer.putInt(data.length);
      buffer.put(data);
    }

    @Override
    public int fromStream(byte[] content, int offset) {
      int len = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[len];
      System.arraycopy(content, offset, data, 0, len);
      offset += len;

      return offset;
    }

    @Override
    public int serializedSize() {
      return data.length + OIntegerSerializer.INT_SIZE;
    }

    @Override
    public int getId() {
      return (byte) 128;
    }
  }
}
