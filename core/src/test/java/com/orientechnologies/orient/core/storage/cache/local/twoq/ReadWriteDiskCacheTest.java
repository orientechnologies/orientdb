package com.orientechnologies.orient.core.storage.cache.local.twoq;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OContextConfiguration;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.*;
import org.assertj.core.api.Assertions;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Random;

public class ReadWriteDiskCacheTest {
  public static final  int userDataSize            = 8;
  public static final  int writeCacheAmountOfPages = 15000;
  private static final int systemOffset            = OIntegerSerializer.INT_SIZE + 3 * OLongSerializer.LONG_SIZE;
  public static final  int PAGE_SIZE               = userDataSize + systemOffset;
  public static final  int READ_CACHE_MAX_MEMORY   = 4 * PAGE_SIZE;
  public static final  int WRITE_CACHE_MAX_SIZE    = writeCacheAmountOfPages * PAGE_SIZE;
  private static O2QCache  readBuffer;
  private static OWOWCache writeBuffer;
  private static OClosableLinkedContainer<Long, OFileClassic> files = new OClosableLinkedContainer<Long, OFileClassic>(1024);

  private static OLocalPaginatedStorage storageLocal;
  private static String                 fileName;
  private static String                 storagePath;
  private static ODiskWriteAheadLog     writeAheadLog;
  private        byte                   seed;

  @BeforeClass
  public static void beforeClass() throws IOException {
    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storagePath = buildDirectory + "/ReadWriteDiskCacheTest";
    storageLocal = (OLocalPaginatedStorage) Orient.instance().getRunningEngine("plocal").createStorage(storagePath, null);
    storageLocal.create(new OContextConfiguration());
    storageLocal.close(true, false);

    fileName = "readWriteDiskCacheTest.tst";

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, WriteAheadLogTest.TestRecord.class);

  }

  @AfterClass
  public static void afterClass() throws IOException {
    if (writeBuffer != null) {
      if (readBuffer != null)
        readBuffer.deleteStorage(writeBuffer);
      else
        writeBuffer.delete();
      writeBuffer = null;
    }

    if (readBuffer != null) {
      readBuffer.clear();
      readBuffer = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    storageLocal.delete();

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/readWriteDiskCacheTest.tst");
    if (file.exists()) {
      Assert.assertTrue(file.delete());
      file.getParentFile().delete();
    }

  }

  @Before
  public void beforeMethod() throws Exception {
    closeBufferAndDeleteFile();

    initBuffer();

    Random random = new Random();
    seed = (byte) (random.nextInt() & 0xFF);
  }

  private void closeBufferAndDeleteFile() throws IOException {
    if (writeBuffer != null) {
      if (readBuffer != null)
        readBuffer.closeStorage(writeBuffer);
      else
        writeBuffer.close();
      writeBuffer = null;
    }

    if (readBuffer != null) {
      readBuffer.clear();
      readBuffer = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }
    files.clear();

    File testFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + "readWriteDiskCacheTest.tst");
    if (testFile.exists()) {
      Assert.assertTrue(testFile.delete());
    }

    File idMapFile = new File(storageLocal.getConfiguration().getDirectory() + File.separator + "name_id_map.cm");
    if (idMapFile.exists()) {
      Assert.assertTrue(idMapFile.delete());
    }
  }

  private void initBuffer() throws IOException, InterruptedException {
    writeBuffer = new OWOWCache(PAGE_SIZE, new OByteBufferPool(PAGE_SIZE), writeAheadLog, -1, WRITE_CACHE_MAX_SIZE, storageLocal,
        false, files, 1);
    writeBuffer.loadRegisteredFiles();

    readBuffer = new O2QCache(READ_CACHE_MAX_MEMORY, PAGE_SIZE, false, 50);
  }

  @Test
  public void testAddFourItems() throws IOException {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      entries[i].markDirty();

      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();

      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 0; i < 4; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(0, 0));
    }
  }

  @Test
  public void testFrequentlyReadItemsAreMovedInAm() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[10];

    for (int i = 0; i < 10; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      entries[i].markDirty();

      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });

      setLsn(buffer, new OLogSequenceNumber(1, i));

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    writeBuffer.flush();
    readBuffer.clear();

    for (int i = 0; i < 10; i++)
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i));

    for (int i = 0; i < 8; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    for (int i = 2; i < 4; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 2);
    Assert.assertEquals(a1in.size(), 2);
    Assert.assertEquals(a1out.size(), 2);

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 2; i < 4; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(am.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      OCacheEntry lruEntry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 6; i < 8; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }
  }

  @Test
  public void testCacheShouldCreateFileIfItIsNotExisted() throws Exception {
    readBuffer.addFile(fileName, writeBuffer);

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/readWriteDiskCacheTest.tst");

    Assert.assertTrue(file.exists());
    Assert.assertTrue(file.isFile());
  }

  @Test
  public void testFrequentlyAddItemsAreMovedInAm() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[10];

    for (int i = 0; i < 10; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      entries[i].markDirty();

      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });
      setLsn(buffer, new OLogSequenceNumber(1, i));

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(a1in.size(), 4);
    Assert.assertEquals(a1out.size(), 2);
    Assert.assertEquals(am.size(), 0);

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 6; i < 10; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      OCacheEntry lruEntry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    Assert.assertEquals(am.size(), 2);
    Assert.assertEquals(a1in.size(), 2);
    Assert.assertEquals(a1out.size(), 2);

    for (int i = 4; i < 6; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(am.get(fileId, i), lruEntry);
    }

    for (int i = 6; i < 8; i++) {
      OCacheEntry lruEntry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 8; i < 10; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }

    writeBuffer.flush();

    for (int i = 0; i < 10; i++)
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i));

  }

  @Test
  public void testReadFourItems() throws IOException {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      entries[i].markDirty();

      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });
      setLsn(buffer, new OLogSequenceNumber(1, i));

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    readBuffer.clear();
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i));
    }

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 0; i < 4; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
  }

  @Test
  public void testPrefetchPagesInA1inQueue() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    for (int i = 0; i < 4; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getSharedBuffer();
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));

        cacheEntry.markDirty();
      } finally {
        readBuffer.releaseFromWrite(cacheEntry, writeBuffer);
      }
    }

    readBuffer.clear();
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      byte[] userData = new byte[userDataSize];
      for (int n = 0; n < userData.length; n++) {
        userData[n] = (byte) (i + 1);
      }
      assertFile(i, userData, new OLogSequenceNumber(1, i));
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 1);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 1);

    readBuffer.clear();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 4);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 4);
  }

  @Test
  public void testPrefetchPagesInA1inAmQueue() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    Assert.assertEquals(readBuffer.getMaxSize(), 4);

    //create file with 8 pages, we will push some of them in different queues later
    for (int i = 0; i < 8; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getSharedBuffer();
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));

        cacheEntry.markDirty();
      } finally {
        readBuffer.releaseFromWrite(cacheEntry, writeBuffer);
      }
    }

    readBuffer.clear();
    writeBuffer.flush();

    for (int i = 0; i < 8; i++) {
      byte[] userData = new byte[userDataSize];
      for (int n = 0; n < userData.length; n++) {
        userData[n] = (byte) (i + 1);
      }
      assertFile(i, userData, new OLogSequenceNumber(1, i));
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    //put 1 and 2  pages to the a1out queue, page 0 is dropped from buffer
    for (int i = 0; i < 7; i++) {
      OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(cacheEntry, writeBuffer);
    }

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 2); // pages 1 - 2
    Assert.assertEquals(a1in.size(), 4); // pages 3 - 6

    //put 1-th page to the am queue
    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 1, false, writeBuffer, 1);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3 (removed from a1in because of size limit)
    Assert.assertEquals(a1in.size(), 3); // pages 4 - 6

    //load page 0 and prefetch 1 (in am) 2 and 3 (in a1out)  and 4 (in a1in) , so 5 pages in total but we have room only for 4
    //am max size is 3 and a1in is 1 if cache is filled, so am will contain pages 2 -3 , a1in page 0, because it is in use,
    //the rest of pages will be removed from cache.

    cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 4);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 3); //pages 1, 2, 3
    Assert.assertEquals(a1out.size(), 2); // pages 5, 6
    Assert.assertEquals(a1in.size(), 1); // page 0

    Assert.assertNotNull(am.get(fileId, 1));
    Assert.assertNotNull(am.get(fileId, 2));
    Assert.assertNotNull(am.get(fileId, 3));

    Assert.assertNotNull(a1out.get(fileId, 5));
    Assert.assertNotNull(a1out.get(fileId, 6));

    Assert.assertNotNull(a1in.get(fileId, 0));
  }

  @Test
  public void testStoreCacheState() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    Assert.assertEquals(readBuffer.getMaxSize(), 4);

    //create file with 8 pages, we will push some of them in different queues later
    for (int i = 0; i < 8; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getSharedBuffer();
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));

        cacheEntry.markDirty();
      } finally {
        readBuffer.releaseFromWrite(cacheEntry, writeBuffer);
      }
    }

    readBuffer.clear();
    writeBuffer.flush();

    for (int i = 0; i < 8; i++) {
      byte[] userData = new byte[userDataSize];
      for (int n = 0; n < userData.length; n++) {
        userData[n] = (byte) (i + 1);
      }
      assertFile(i, userData, new OLogSequenceNumber(1, i));
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    //put 1 and 2  pages to the a1out queue, page 0 is dropped from buffer
    for (int i = 0; i < 7; i++) {
      OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(cacheEntry, writeBuffer);
    }

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 2); // pages 1 - 2
    Assert.assertEquals(a1in.size(), 4); // pages 3 - 6

    //put 1-th page to the am queue
    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 1, false, writeBuffer, 1);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3 (removed from a1in because of size limit)
    Assert.assertEquals(a1in.size(), 3); // pages 4 - 6

    readBuffer.storeCacheState(writeBuffer);
    readBuffer.closeStorage(writeBuffer);

    final File stateFile = new File(storagePath, O2QCache.CACHE_STATE_FILE);
    Assert.assertTrue(stateFile.exists());

    initBuffer();
    readBuffer.loadCacheState(writeBuffer);

    am = readBuffer.getAm();
    a1in = readBuffer.getA1in();
    a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3 (removed from a1in because of size limit)
    Assert.assertEquals(a1in.size(), 3); // pages 4 - 6

    for (OCacheEntry entry : am) {
      Assert.assertEquals(entry.getFileId(), fileId);
      Assert.assertEquals(entry.getPageIndex(), 1);
      Assert.assertNotNull(entry.getCachePointer());
    }

    int counter = 3;
    for (OCacheEntry entry : a1out) {
      Assert.assertEquals(entry.getFileId(), fileId);
      Assert.assertEquals(entry.getPageIndex(), counter);
      Assert.assertNull(entry.getCachePointer());
      counter--;
    }

    counter = 6;
    for (OCacheEntry entry : a1in) {
      Assert.assertEquals(entry.getFileId(), fileId);
      Assert.assertEquals(entry.getPageIndex(), counter);
      Assert.assertNotNull(entry.getCachePointer());
      counter--;
    }

  }

  @Test
  public void testPrefetchPagesInPinnedPages() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    Assert.assertEquals(readBuffer.getMaxSize(), 4);

    //create file with 8 pages, we will push some of them in different queues later
    for (int i = 0; i < 8; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getSharedBuffer();
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));

        cacheEntry.markDirty();
      } finally {
        readBuffer.releaseFromWrite(cacheEntry, writeBuffer);
      }
    }

    readBuffer.clear();
    writeBuffer.flush();

    for (int i = 0; i < 8; i++) {
      byte[] userData = new byte[userDataSize];
      for (int n = 0; n < userData.length; n++) {
        userData[n] = (byte) (i + 1);
      }
      assertFile(i, userData, new OLogSequenceNumber(1, i));
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    //put 1 and 2  pages to the a1out queue, page 0 is dropped from buffer
    for (int i = 0; i < 7; i++) {
      OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1);
      readBuffer.releaseFromRead(cacheEntry, writeBuffer);
    }

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 2); // pages 1 - 2
    Assert.assertEquals(a1in.size(), 4); // pages 3 - 6

    //put 1-th page to the am queue
    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 1, false, writeBuffer, 1);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3 (removed from a1in because of size limit)
    Assert.assertEquals(a1in.size(), 3); // pages 4 - 6

    //move page 4 to pinned pages
    cacheEntry = readBuffer.loadForRead(fileId, 4, false, writeBuffer, 1);
    readBuffer.pinPage(cacheEntry);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3
    Assert.assertEquals(a1in.size(), 2); // pages 5 - 6

    //change cache max size so at least single page may be put in a1in queue
    readBuffer.changeMaximumAmountOfMemory(5 * (PAGE_SIZE));

    //load page 0, and prefetch pages 1 (am), 2,3 (a1out) and 4 (pinned page)
    //so 5 pages in total, but we may only load 4 pages (1 page is pinned),so we have
    //page 0 in a1in (as used page), pages 1, 2, 3 in am, pages 5, 6 in a1out

    cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 4);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 3); //page 1,2,3
    Assert.assertEquals(a1out.size(), 2); // pages 5, 6
    Assert.assertEquals(a1in.size(), 1); // page 0

    Assert.assertTrue(readBuffer.inPinnedPages(fileId, 4));

    Assert.assertNotNull(am.get(fileId, 1));
    Assert.assertNotNull(am.get(fileId, 2));
    Assert.assertNotNull(am.get(fileId, 3));

    Assert.assertNotNull(a1out.get(fileId, 5));
    Assert.assertNotNull(a1out.get(fileId, 6));

    Assert.assertNotNull(a1in.get(fileId, 0));

    readBuffer.changeMaximumAmountOfMemory(4 * (PAGE_SIZE));
  }

  @Test
  public void testLoadAndLockForReadShouldHitCache() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry cacheEntry = readBuffer.loadForWrite(fileId, 0, false, writeBuffer, 1);
    if (cacheEntry == null) {
      cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer);
      Assert.assertEquals(cacheEntry.getPageIndex(), 0);
    }

    readBuffer.releaseFromWrite(cacheEntry, writeBuffer);

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    final OCacheEntry entry = generateEntry(fileId, 0, cacheEntry.getCachePointer().getSharedBuffer(), bufferPool, false);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
  }

  @Test
  public void testCloseFileShouldFlushData() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      entries[i].markDirty();
      ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 0; i < 4; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
    readBuffer.closeFile(fileId, true, writeBuffer);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(0, 0));
    }
  }

  @Test
  public void testDeleteFileShouldDeleteFileFromHardDrive() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[4];

    byte[][] content = new byte[4][];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      buffer.position(systemOffset);
      content[i] = new byte[8];
      buffer.get(content[i]);

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    readBuffer.deleteFile(fileId, writeBuffer);
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      File file = new File(storageLocal.getConfiguration().getDirectory() + "/readWriteDiskCacheTest.tst");
      Assert.assertFalse(file.exists());
    }
  }

  @Test
  public void testFlushData() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; ++j) {
        entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
        if (entries[i] == null) {
          entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
          Assert.assertEquals(entries[i].getPageIndex(), i);
        }

        entries[i].markDirty();

        final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
        buffer.position(systemOffset);
        buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, (byte) j, (byte) i });

        readBuffer.releaseFromWrite(entries[i], writeBuffer);
      }
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 0; i < 4; i++) {
      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      OCacheEntry entry = generateEntry(fileId, i, buffer, bufferPool, false);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);

    writeBuffer.flush(fileId);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 3, (byte) i }, new OLogSequenceNumber(0, 0));
    }

  }

  @Test
  public void testIfNotEnoughSpaceOldPagesShouldBeMovedToA1Out() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[6];

    for (int i = 0; i < 6; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      entries[i].markDirty();

      final ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 });

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);

    for (int i = 0; i < 2; i++) {
      OCacheEntry entry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    final OByteBufferPool bufferPool = OByteBufferPool.instance();
    for (int i = 2; i < 6; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getSharedBuffer(), bufferPool, false);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 6);
    writeBuffer.flush();

    for (int i = 0; i < 6; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, new OLogSequenceNumber(0, 0));
    }
  }

  @Test(expected = OAllCacheEntriesAreUsedException.class)
  public void testIfAllPagesAreUsedExceptionShouldBeThrown() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[5];
    try {
      for (int i = 0; i < 5; i++) {
        entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
        if (entries[i] == null) {
          entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer);
          Assert.assertEquals(entries[i].getPageIndex(), i);
        }

        entries[i].markDirty();

        ByteBuffer buffer = entries[i].getCachePointer().getSharedBuffer();
        buffer.position(systemOffset);
        buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 });

        if (i - 4 >= 0) {
          readBuffer.loadForWrite(fileId, i - 4, false, writeBuffer, 0);

          buffer = entries[i - 4].getCachePointer().getSharedBuffer();
          buffer.position(systemOffset);
          buffer.put(new byte[] { (byte) (i - 4), 1, 2, seed, 4, 5, 6, 7 });
        }
      }
    } finally {
      for (int i = 0; i < 4; i++) {
        readBuffer.releaseFromWrite(entries[i], writeBuffer);
      }
    }
  }

  @Test
  @Ignore
  public void testFlushTillLSN() throws Exception {
    closeBufferAndDeleteFile();

    File file = new File(storageLocal.getConfiguration().getDirectory());
    if (!file.exists())
      file.mkdir();

    writeAheadLog = new ODiskWriteAheadLog(1024, -1, 10 * 1024, null, true, storageLocal, 16 * OWALPage.PAGE_SIZE, 120);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "readWriteDiskCacheTest.tst", 0);
    segmentConfiguration.fileType = OFileClassic.NAME;

    writeBuffer = new OWOWCache(8 + systemOffset, new OByteBufferPool(8 + systemOffset), writeAheadLog, 100,
        2 * (8 + systemOffset),  storageLocal, false, files, 10);

    writeBuffer.loadRegisteredFiles();

    readBuffer = new O2QCache(4 * (8 + systemOffset), 8 + systemOffset, false, 20);

    long fileId = readBuffer.addFile(fileName, writeBuffer);
    OLogSequenceNumber lsnToFlush = null;

    for (int i = 0; i < 8; i++) {
      OCacheEntry cacheEntry = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1);
      if (cacheEntry == null) {
        cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer);
        Assert.assertEquals(cacheEntry.getPageIndex(), i);
      }
      OCachePointer dataPointer = cacheEntry.getCachePointer();

      OLogSequenceNumber pageLSN = writeAheadLog.log(new WriteAheadLogTest.TestRecord(30, false));

      setLsn(dataPointer.getSharedBuffer(), pageLSN);

      lsnToFlush = pageLSN;

      cacheEntry.markDirty();
      readBuffer.releaseFromWrite(cacheEntry, writeBuffer);

    }

    Thread.sleep(1000);

    Assert.assertEquals(writeAheadLog.getFlushedLsn(), lsnToFlush);
  }

  private void updateFilePage(long pageIndex, long offset, byte[] value) throws IOException {
    OFileClassic fileClassic = new OFileClassic(Paths.get(storageLocal.getConfiguration().getDirectory(), "readWriteDiskCacheTest.tst"));
    fileClassic.open();

    fileClassic.write(pageIndex * (8 + systemOffset) + offset, value, value.length, 0);
    fileClassic.synch();
    fileClassic.close();
  }

  private void assertFile(long pageIndex, byte[] value, OLogSequenceNumber lsn) throws IOException {
    OFileClassic fileClassic = new OFileClassic(Paths.get(storageLocal.getConfiguration().getDirectory(), "readWriteDiskCacheTest.tst"));
    fileClassic.open();
    byte[] content = new byte[userDataSize + systemOffset];
    fileClassic.read(pageIndex * (userDataSize + systemOffset), content, userDataSize + systemOffset);

    Assertions.assertThat(Arrays.copyOfRange(content, systemOffset, userDataSize + systemOffset)).isEqualTo(value);

    long segment = OLongSerializer.INSTANCE.deserializeNative(content, ODurablePage.WAL_SEGMENT_OFFSET);
    long position = OLongSerializer.INSTANCE.deserializeNative(content, ODurablePage.WAL_POSITION_OFFSET);

    OLogSequenceNumber readLsn = new OLogSequenceNumber(segment, position);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

  private OCacheEntry generateEntry(long fileId, long pageIndex, ByteBuffer buffer, OByteBufferPool bufferPool, boolean dirty) {
    return new OCacheEntryImpl(fileId, pageIndex, new OCachePointer(buffer, bufferPool, fileId, pageIndex), dirty);
  }

  private OCacheEntry generateRemovedEntry(long fileId, long pageIndex) {
    return new OCacheEntryImpl(fileId, pageIndex, null, false);
  }

  private void setLsn(ByteBuffer buffer, OLogSequenceNumber lsn) {
    buffer.position(OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

    buffer.putLong(lsn.getSegment());
    buffer.putLong(lsn.getPosition());
  }
}
