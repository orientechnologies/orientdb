package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import java.util.zip.CRC32;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODiskWriteAheadLog;
import org.apache.commons.lang.SystemUtils;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WriteAheadLogTest;

/**
 * @author Andrey Lomakin
 * @since 26.07.13
 */
@Test
public class WOWCacheTest {
  private int systemOffset = 2 * (OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);
  private int pageSize     = systemOffset + 8;

  private OLocalPaginatedStorage storageLocal;
  private String                 fileName;

  private ODiskWriteAheadLog writeAheadLog;

  private OWOWCache wowCache;
  private OClosableLinkedContainer<Long, OFileClassic> files = new OClosableLinkedContainer<Long, OFileClassic>(1024);

  @BeforeClass
  public void beforeClass() throws IOException {
    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OLocalPaginatedStorage) Orient.instance().loadStorage("plocal:" + buildDirectory + "/WOWCacheTest");
    storageLocal.create(null);

    fileName = "wowCacheTest.tst";

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, WriteAheadLogTest.TestRecord.class);
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    closeCacheAndDeleteFile();

    initBuffer();
  }

  private void closeCacheAndDeleteFile() throws IOException {
    if (wowCache != null) {
      wowCache.close();
      wowCache = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    storageLocal.delete();

    File testFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + fileName);
    if (testFile.exists()) {
      Assert.assertTrue(testFile.delete());
    }

    File nameIdMapFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + "name_id_map.cm");
    if (nameIdMapFile.exists()) {
      Assert.assertTrue(nameIdMapFile.delete());
    }
  }

  @AfterClass
  public void afterClass() throws IOException {
    closeCacheAndDeleteFile();

    File file = new File(storageLocal.getConfiguration().getDirectory());
    Assert.assertTrue(file.delete());
  }

  private void initBuffer() throws IOException {
    wowCache = new OWOWCache(true, pageSize, new OByteBufferPool(pageSize), 10000, writeAheadLog, 10, 100, 100, storageLocal, false,
        files, 1);
    wowCache.loadRegisteredFiles();
  }

  public void testLoadStore() throws IOException {
    Random random = new Random();

    byte[][] pageData = new byte[200][];
    long fileId = wowCache.addFile(fileName);

    for (int i = 0; i < pageData.length; i++) {
      byte[] data = new byte[8];
      random.nextBytes(data);

      pageData[i] = data;

      final OCachePointer cachePointer = wowCache.load(fileId, i, 1, true, new OModifiableBoolean())[0];
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

      OCachePointer cachePointer = wowCache.load(fileId, i, 1, false, new OModifiableBoolean())[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertEquals(dataTwo, dataOne);
    }

    wowCache.flush();

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataContent = pageData[i];
      assertFile(i, dataContent, new OLogSequenceNumber(0, 0));
    }
  }

  public void testDataUpdate() throws Exception {
    final NavigableMap<Long, byte[]> pageIndexDataMap = new TreeMap<Long, byte[]>();
    long fileId = wowCache.addFile(fileName);

    Random random = new Random();

    for (int i = 0; i < 600; i++) {
      long pageIndex = random.nextInt(2048);

      byte[] data = new byte[8];
      random.nextBytes(data);

      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, true, new OModifiableBoolean())[0];
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

      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, false, new OModifiableBoolean())[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);

      cachePointer.decrementReadersReferrer();
      Assert.assertEquals(dataTwo, dataOne);
    }

    for (int i = 0; i < 300; i++) {
      long desiredIndex = random.nextInt(2048);

      Long pageIndex = pageIndexDataMap.ceilingKey(desiredIndex);
      if (pageIndex == null)
        pageIndex = pageIndexDataMap.floorKey(desiredIndex);

      byte[] data = new byte[8];
      random.nextBytes(data);
      pageIndexDataMap.put(pageIndex, data);

      final OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, true, new OModifiableBoolean())[0];

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
      OCachePointer cachePointer = wowCache.load(fileId, pageIndex, 1, false, new OModifiableBoolean())[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertEquals(dataTwo, dataOne);
    }

    wowCache.flush();

    for (Map.Entry<Long, byte[]> entry : pageIndexDataMap.entrySet()) {
      assertFile(entry.getKey(), entry.getValue(), new OLogSequenceNumber(0, 0));
    }

  }

  public void testFlushAllContentEventually() throws Exception {
    Random random = new Random();

    byte[][] pageData = new byte[200][];
    long fileId = wowCache.addFile(fileName);

    for (int i = 0; i < pageData.length; i++) {
      byte[] data = new byte[8];
      random.nextBytes(data);

      pageData[i] = data;

      final OCachePointer cachePointer = wowCache.load(fileId, i, 1, true, new OModifiableBoolean())[0];
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

      OCachePointer cachePointer = wowCache.load(fileId, i, 1, false, new OModifiableBoolean())[0];
      byte[] dataTwo = new byte[8];
      ByteBuffer buffer = cachePointer.getSharedBuffer();
      buffer.position(systemOffset);
      buffer.get(dataTwo);
      cachePointer.decrementReadersReferrer();

      Assert.assertEquals(dataTwo, dataOne);
    }

    final long start = System.currentTimeMillis();
    while (wowCache.getWriteCacheSize() != 0) {
      Thread.sleep(1000);

      //wait no more than 10 min
      if (((System.currentTimeMillis() - start) / 1000) > 10 * 60) {
        Assert.assertEquals(wowCache.getWriteCacheSize(), 0);
      }
    }

    for (int i = 0; i < pageData.length; i++) {
      byte[] dataContent = pageData[i];
      assertFile(i, dataContent, new OLogSequenceNumber(0, 0));
    }
  }

  public void testFileRestore() throws IOException {
    final long nonDelFileId = wowCache.addFile(fileName);
    final long fileId = wowCache.addFile("removedFile.del");

    wowCache.deleteFile(fileId);
    File deletedFile = new File(storageLocal.getStoragePath(), "removedFile.del");
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

  public void testFileRestoreAfterClose() throws IOException {
    final long nonDelFileId = wowCache.addFile(fileName);
    final long fileId = wowCache.addFile("removedFile.del");

    wowCache.deleteFile(fileId);
    File deletedFile = new File(storageLocal.getStoragePath(), "removedFile.del");
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

  private void assertFile(long pageIndex, byte[] value, OLogSequenceNumber lsn) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + File.separator + fileName;

    OFileClassic fileClassic = new OFileClassic(path, "r");
    fileClassic.open();
    byte[] content = new byte[8 + systemOffset];
    fileClassic.read(pageIndex * (8 + systemOffset), content, 8 + systemOffset);

    Assert.assertEquals(Arrays.copyOfRange(content, systemOffset, 8 + systemOffset), value);

    long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, 0);

    Assert.assertEquals(magicNumber, OWOWCache.MAGIC_NUMBER);
    CRC32 crc32 = new CRC32();
    crc32.update(content, OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE,
        content.length - OIntegerSerializer.INT_SIZE - OLongSerializer.LONG_SIZE);

    int crc = OIntegerSerializer.INSTANCE.deserializeNative(content, OLongSerializer.LONG_SIZE);
    Assert.assertEquals(crc, (int) crc32.getValue());

    int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);
    long position = OLongSerializer.INSTANCE
        .deserializeNative(content, OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE);

    OLogSequenceNumber readLsn = new OLogSequenceNumber(segment, position);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

}
