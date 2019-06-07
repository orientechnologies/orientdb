package com.orientechnologies.orient.core.storage.cache.local.twoq;

import com.orientechnologies.common.collection.closabledictionary.OClosableLinkedContainer;
import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.common.serialization.types.OStringSerializer;
import com.orientechnologies.common.types.OModifiableBoolean;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OAllCacheEntriesAreUsedException;
import com.orientechnologies.orient.core.storage.OChecksumMode;
import com.orientechnologies.orient.core.storage.cache.OCacheEntry;
import com.orientechnologies.orient.core.storage.cache.OCacheEntryImpl;
import com.orientechnologies.orient.core.storage.cache.OCachePointer;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurablePage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas.OCASDiskWriteAheadLog;
import org.assertj.core.api.Assertions;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.*;

public class ReadWriteDiskCacheTest {
  private static final int                   userDataSize            = 8;
  private static final int                   writeCacheAmountOfPages = 15000;
  private static final int                   systemOffset            = OIntegerSerializer.INT_SIZE + 3 * OLongSerializer.LONG_SIZE;
  private static final int                   PAGE_SIZE               = userDataSize + systemOffset;
  private static final int                   READ_CACHE_MAX_MEMORY   = 4 * PAGE_SIZE;
  private static final int                   WRITE_CACHE_MAX_SIZE    = writeCacheAmountOfPages * PAGE_SIZE;
  private static       O2QCache              readBuffer;
  private static       OWOWCache             writeBuffer;
  private static       OCASDiskWriteAheadLog writeAheadLog;

  private static final OClosableLinkedContainer<Long, OFileClassic> files = new OClosableLinkedContainer<>(1024);

  private static String fileName;
  private static Path   storagePath;
  private static String storageName;
  private        byte   seed;

  private static final OByteBufferPool BUFFER_POOL = new OByteBufferPool(PAGE_SIZE);

  @BeforeClass
  public static void beforeClass() {
    OGlobalConfiguration.STORAGE_EXCLUSIVE_FILE_ACCESS.setValue(Boolean.FALSE);

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = ".";
    }

    storageName = "ReadWriteDiskCacheTest";
    storagePath = Paths.get(buildDirectory).resolve("ReadWriteDiskCacheTest");
    fileName = "readWriteDiskCacheTest.tst";

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);

  }

  @AfterClass
  public static void afterClass() throws IOException {
    long fileId = -1;

    if (writeBuffer != null) {
      fileId = writeBuffer.fileIdByName("readWriteDiskCacheTest.tst");
    }

    String nativeFileName = null;

    if (fileId >= 0) {
      nativeFileName = writeBuffer.nativeFileNameById(fileId);
    }

    if (writeBuffer != null) {
      if (readBuffer != null) {
        readBuffer.deleteStorage(writeBuffer);
      } else {
        writeBuffer.delete();
      }

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

    if (nativeFileName != null) {
      final File file = storagePath.resolve(nativeFileName).toFile();

      if (file.exists()) {
        Assert.assertTrue(file.delete());
        Assert.assertTrue(file.getParentFile().delete());
      }
    }

    BUFFER_POOL.clear();
    OGlobalConfiguration.STORAGE_EXCLUSIVE_FILE_ACCESS.setValue(Boolean.TRUE);
  }

  @Before
  public void beforeMethod() throws Exception {
    closeBufferAndDeleteFile();

    initBuffer();

    Random random = new Random();
    seed = (byte) (random.nextInt() & 0xFF);
  }

  private void closeBufferAndDeleteFile() throws IOException {
    long fileId = -1;

    if (writeBuffer != null) {
      fileId = writeBuffer.fileIdByName("readWriteDiskCacheTest.tst");
    }

    String nativeFileName = null;

    if (fileId >= 0) {
      nativeFileName = writeBuffer.nativeFileNameById(fileId);
    }

    if (writeBuffer != null) {
      if (readBuffer != null) {
        readBuffer.deleteStorage(writeBuffer);
      } else {
        writeBuffer.delete();
      }

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

    if (nativeFileName != null) {
      final File testFile = storagePath.resolve(nativeFileName).toFile();

      if (testFile.exists()) {
        Assert.assertTrue(testFile.delete());
      }
    }

    File idMapFile = storagePath.resolve("name_id_map.cm").toFile();
    if (idMapFile.exists()) {
      Assert.assertTrue(idMapFile.delete());
    }

    idMapFile = storagePath.resolve("name_id_map_v2.cm").toFile();
    if (idMapFile.exists()) {
      Assert.assertTrue(idMapFile.delete());
    }
  }

  private void initBuffer() throws IOException, InterruptedException {
    writeAheadLog = new OCASDiskWriteAheadLog(storageName, storagePath, storagePath, 12_000, 128, Integer.MAX_VALUE,
        Integer.MAX_VALUE, 25, true, Locale.US, -1, 1024L * 1024 * 1024, 1000, true, false, true, 10);

    writeBuffer = new OWOWCache(PAGE_SIZE, BUFFER_POOL, writeAheadLog, -1, 10, WRITE_CACHE_MAX_SIZE, storagePath, storageName,
        OStringSerializer.INSTANCE, files, 1, OChecksumMode.StoreAndThrow, null, null, false, true, 10);
    writeBuffer.loadRegisteredFiles();

    readBuffer = new O2QCache(READ_CACHE_MAX_MEMORY, PAGE_SIZE, false, 50, true, 10);
  }

  @Test
  public void testAddFourItems() throws IOException {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getBuffer();

      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 0; i < 4; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(0, 0), nativeFileName);
    }
  }

  private void addFileName(String fileName, List<Long> fileIds, List<String> nativeFileNames) throws IOException {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    fileIds.add(fileId);
    nativeFileNames.add(nativeFileName);
  }

  @SuppressWarnings("SpellCheckingInspection")
  @Test
  public void testAddFourItemsInFourDifferentFiles() throws IOException {
    List<Long> fileIds = new ArrayList<>();
    List<String> nativeFileNames = new ArrayList<>();
    List<String> fileNames = new ArrayList<>();

    fileNames.add("readWriteDiskCacheTest.tst");
    fileNames.add("readwRitedIskCacheTest.tst");
    fileNames.add("ReadwRitedIskCacheTesT.tst");
    fileNames.add("readwritediskcachetest.tst");

    addFileName(fileNames.get(0), fileIds, nativeFileNames);
    addFileName(fileNames.get(1), fileIds, nativeFileNames);
    addFileName(fileNames.get(2), fileIds, nativeFileNames);
    addFileName(fileNames.get(3), fileIds, nativeFileNames);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      for (int n = 0; n < 4; n++) {
        final long fileId = fileIds.get(n);

        entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
        if (entries[i] == null) {
          entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
          Assert.assertEquals(entries[i].getPageIndex(), i);
        }

        entries[i].getCachePointer().acquireExclusiveLock();

        final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();

        assert buffer != null;
        buffer.position(systemOffset);
        buffer.put(new byte[] { (byte) i, 1, 2, (byte) (seed + n), 4, 5, 6, (byte) (i + n) });

        entries[i].getCachePointer().releaseExclusiveLock();

        readBuffer.releaseFromWrite(entries[i], writeBuffer);
      }
    }

    for (int n = 0; n < 4; n++) {
      final long fileId = fileIds.get(n);
      Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
    }

    writeBuffer.flush();

    for (int n = 0; n < 4; n++) {
      final String nativeFileName = nativeFileNames.get(n);

      for (int i = 0; i < 4; i++) {
        assertFile(i, new byte[] { (byte) i, 1, 2, (byte) (seed + n), 4, 5, 6, (byte) (i + n) }, new OLogSequenceNumber(0, 0),
            nativeFileName);
      }
    }

    for (int n = 3; n >= 0; n--) {
      for (int i = 3; i >= 0; i--) {
        final String fileNativeName = nativeFileNames.get(i);

        final File file = storagePath.resolve(fileNativeName).toFile();
        if (i > n) {
          Assert.assertFalse(file.exists());
        } else {
          Assert.assertTrue(file.exists());
        }
      }

      writeBuffer.deleteFile(fileIds.get(n));

      for (int i = 3; i >= 0; i--) {
        final String fileNativeName = nativeFileNames.get(i);

        final File file = storagePath.resolve(fileNativeName).toFile();
        if (i >= n) {
          Assert.assertFalse(file.exists());
        } else {
          Assert.assertTrue(file.exists());
        }
      }
    }
  }

  @SuppressWarnings("SpellCheckingInspection")
  @Test
  public void testAddFourItemsInFourDifferentFilesCloseAndOpen() throws Exception {
    List<Long> fileIds = new ArrayList<>();
    List<String> nativeFileNames = new ArrayList<>();
    List<String> fileNames = new ArrayList<>();

    fileNames.add("readWriteDiskCacheTest.tst");
    fileNames.add("readwRitedIskCacheTest.tst");
    fileNames.add("ReadwRitedIskCacheTesT.tst");
    fileNames.add("readwritediskcachetest.tst");

    addFileName(fileNames.get(0), fileIds, nativeFileNames);
    addFileName(fileNames.get(1), fileIds, nativeFileNames);
    addFileName(fileNames.get(2), fileIds, nativeFileNames);
    addFileName(fileNames.get(3), fileIds, nativeFileNames);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      for (int n = 0; n < 4; n++) {
        final long fileId = fileIds.get(n);

        entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);

        if (entries[i] == null) {
          entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        }

        entries[i].getCachePointer().acquireExclusiveLock();

        final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();

        assert buffer != null;
        buffer.position(systemOffset);
        buffer.put(new byte[] { (byte) i, 1, 2, (byte) (seed + n), 4, 5, 6, (byte) (i + n) });

        entries[i].getCachePointer().releaseExclusiveLock();

        readBuffer.releaseFromWrite(entries[i], writeBuffer);
      }
    }

    readBuffer.closeStorage(writeBuffer);
    writeAheadLog.close();

    initBuffer();

    fileIds.clear();

    for (int n = 0; n < 4; n++) {
      fileIds.add(writeBuffer.loadFile(fileNames.get(n)));

      final String nativeFileName = writeBuffer.nativeFileNameById(fileIds.get(n));
      Assert.assertEquals(nativeFileName, nativeFileNames.get(n));
    }

    for (int n = 0; n < 4; n++) {
      final long fileId = fileIds.get(n);
      Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
    }

    for (int n = 3; n >= 0; n--) {
      for (int i = 3; i >= 0; i--) {
        final String fileNativeName = nativeFileNames.get(i);

        final File file = storagePath.resolve(fileNativeName).toFile();
        if (i > n) {
          Assert.assertFalse(file.exists());
        } else {
          Assert.assertTrue(file.exists());
        }
      }

      writeBuffer.deleteFile(fileIds.get(n));

      for (int i = 3; i >= 0; i--) {
        final String fileNativeName = nativeFileNames.get(i);

        final File file = storagePath.resolve(fileNativeName).toFile();
        if (i >= n) {
          Assert.assertFalse(file.exists());
        } else {
          Assert.assertTrue(file.exists());
        }
      }
    }
  }

  @Test
  @Ignore
  public void testFrequentlyReadItemsAreMovedInAm() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[10];

    for (int i = 0; i < 10; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
      assert buffer != null;
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });

      setLsn(buffer, new OLogSequenceNumber(1, i));

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    writeBuffer.flush();
    readBuffer.clear();

    for (int i = 0; i < 10; i++)
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i), nativeFileName);

    for (int i = 0; i < 8; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1, true);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    for (int i = 2; i < 4; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1, true);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 2);
    Assert.assertEquals(a1in.size(), 2);
    Assert.assertEquals(a1out.size(), 2);

    final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 2; i < 4; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(am.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      OCacheEntry lruEntry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 6; i < 8; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }
  }

  @Test
  public void testCacheShouldCreateFileIfItIsNotExisted() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    File file = storagePath.resolve(nativeFileName).toFile();
    Assert.assertTrue(file.exists());
    Assert.assertTrue(file.isFile());
  }

  @Test
  @Ignore
  public void testFrequentlyAddItemsAreMovedInAm() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[10];

    for (int i = 0; i < 10; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
      assert buffer != null;
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

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 6; i < 10; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      OCacheEntry lruEntry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1, true);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    Assert.assertEquals(am.size(), 2);
    Assert.assertEquals(a1in.size(), 2);
    Assert.assertEquals(a1out.size(), 2);

    for (int i = 4; i < 6; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(am.get(fileId, i), lruEntry);
    }

    for (int i = 6; i < 8; i++) {
      OCacheEntry lruEntry = generateRemovedEntry(fileId, i);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 8; i < 10; i++) {
      OCacheEntry lruEntry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }

    writeBuffer.flush();

    for (int i = 0; i < 10; i++)
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i), nativeFileName);

  }

  @Test
  public void testReadFourItems() throws IOException {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
      assert buffer != null;
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });
      setLsn(buffer, new OLogSequenceNumber(1, i));

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    readBuffer.clear();
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i), nativeFileName);
    }

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1, true);
      readBuffer.releaseFromRead(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 0; i < 4; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
  }

  @Test
  public void testPrefetchPagesInA1inQueue() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    for (int i = 0; i < 4; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer, null);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
        assert buffer != null;
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));
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
      assertFile(i, userData, new OLogSequenceNumber(1, i), nativeFileName);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 1, true);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 1);

    readBuffer.clear();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 4, true);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 4);
  }

  @Test
  @Ignore
  public void testPrefetchPagesInA1inAmQueue() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    Assert.assertEquals(readBuffer.getMaxSize(), 4);

    //create file with 8 pages, we will push some of them in different queues later
    for (int i = 0; i < 8; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer, null);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
        assert buffer != null;
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));
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
      assertFile(i, userData, new OLogSequenceNumber(1, i), nativeFileName);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    //put 1 and 2  pages to the a1out queue, page 0 is dropped from buffer
    for (int i = 0; i < 7; i++) {
      OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1, true);
      readBuffer.releaseFromRead(cacheEntry, writeBuffer);
    }

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 2); // pages 1 - 2
    Assert.assertEquals(a1in.size(), 4); // pages 3 - 6

    //put 1-th page to the am queue
    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 1, false, writeBuffer, 1, true);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3 (removed from a1in because of size limit)
    Assert.assertEquals(a1in.size(), 3); // pages 4 - 6

    //load page 0 and prefetch 1 (in am) 2 and 3 (in a1out)  and 4 (in a1in) , so 5 pages in total but we have room only for 4
    //am max size is 3 and a1in is 1 if cache is filled, so am will contain pages 2 -3 , a1in page 0, because it is in use,
    //the rest of pages will be removed from cache.

    cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 4, true);
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
  @Ignore
  public void testPrefetchPagesInPinnedPages() throws Exception {
    final long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    Assert.assertEquals(readBuffer.getMaxSize(), 4);

    //create file with 8 pages, we will push some of them in different queues later
    for (int i = 0; i < 8; i++) {
      OCacheEntry cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer, null);
      try {
        byte[] userData = new byte[userDataSize];
        for (int n = 0; n < userData.length; n++) {
          userData[n] = (byte) (i + 1);
        }

        final ByteBuffer buffer = cacheEntry.getCachePointer().getBufferDuplicate();
        assert buffer != null;
        buffer.position(systemOffset);
        buffer.put(userData);

        setLsn(buffer, new OLogSequenceNumber(1, i));
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
      assertFile(i, userData, new OLogSequenceNumber(1, i), nativeFileName);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    Assert.assertEquals(a1in.size(), 0);

    //put 1 and 2  pages to the a1out queue, page 0 is dropped from buffer
    for (int i = 0; i < 7; i++) {
      OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, i, false, writeBuffer, 1, true);
      readBuffer.releaseFromRead(cacheEntry, writeBuffer);
    }

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 2); // pages 1 - 2
    Assert.assertEquals(a1in.size(), 4); // pages 3 - 6

    //put 1-th page to the am queue
    OCacheEntry cacheEntry = readBuffer.loadForRead(fileId, 1, false, writeBuffer, 1, true);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3 (removed from a1in because of size limit)
    Assert.assertEquals(a1in.size(), 3); // pages 4 - 6

    //move page 4 to pinned pages
    cacheEntry = readBuffer.loadForRead(fileId, 4, false, writeBuffer, 1, true);
    readBuffer.pinPage(cacheEntry, writeBuffer);
    readBuffer.releaseFromRead(cacheEntry, writeBuffer);

    Assert.assertEquals(am.size(), 1); //page 1
    Assert.assertEquals(a1out.size(), 2); // page 2 - 3
    Assert.assertEquals(a1in.size(), 2); // pages 5 - 6

    //change cache max size so at least single page may be put in a1in queue
    readBuffer.changeMaximumAmountOfMemory(5 * (PAGE_SIZE));

    //load page 0, and prefetch pages 1 (am), 2,3 (a1out) and 4 (pinned page)
    //so 5 pages in total, but we may only load 4 pages (1 page is pinned),so we have
    //page 0 in a1in (as used page), pages 1, 2, 3 in am, pages 5, 6 in a1out

    cacheEntry = readBuffer.loadForRead(fileId, 0, false, writeBuffer, 4, true);
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

    OCacheEntry cacheEntry = readBuffer.loadForWrite(fileId, 0, false, writeBuffer, 1, true, null);
    if (cacheEntry == null) {
      cacheEntry = readBuffer.allocateNewPage(fileId, writeBuffer, null);
      Assert.assertEquals(cacheEntry.getPageIndex(), 0);
    }

    readBuffer.releaseFromWrite(cacheEntry, writeBuffer);

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    final OCacheEntry entry = generateEntry(fileId, 0, cacheEntry.getCachePointer().getPointer(), bufferPool);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
  }

  @Test
  public void testCloseFileShouldFlushData() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }
      ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
      assert buffer != null;
      buffer.position(systemOffset);
      buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    LRUList am = readBuffer.getAm();
    LRUList a1in = readBuffer.getA1in();
    LRUList a1out = readBuffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 0; i < 4; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);
    readBuffer.closeFile(fileId, true, writeBuffer);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(0, 0), nativeFileName);
    }
  }

  @Test
  public void testDeleteFileShouldDeleteFileFromHardDrive() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[4];

    byte[][] content = new byte[4][];

    for (int i = 0; i < 4; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
      assert buffer != null;
      buffer.position(systemOffset);
      content[i] = new byte[8];
      buffer.get(content[i]);

      readBuffer.releaseFromWrite(entries[i], writeBuffer);
    }

    readBuffer.deleteFile(fileId, writeBuffer);
    writeBuffer.flush();

    for (int i = 0; i < 4; i++) {
      File file = storagePath.resolve(nativeFileName).toFile();
      Assert.assertFalse(file.exists());
    }
  }

  @Test
  public void testFileContentReplacement() throws IOException {
    // Add a file.

    final long fileId = writeBuffer.addFile(fileName);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);
    final Path path = storagePath.resolve(nativeFileName);
    Assert.assertTrue(Files.exists(path));

    // Set the file content to random.

    writeBuffer.allocateNewPage(fileId);
    final OCachePointer cachePointer = writeBuffer.load(fileId, 0, 1, new OModifiableBoolean(), false)[0];
    cachePointer.acquireExclusiveLock();
    final Random random = new Random(seed);
    final ByteBuffer buffer = cachePointer.getBufferDuplicate();
    assert buffer != null;
    Assert.assertTrue(buffer.limit() > systemOffset);
    for (int i = systemOffset; i < buffer.limit(); ++i)
      buffer.put(i, (byte) random.nextInt());
    cachePointer.releaseExclusiveLock();

    writeBuffer.store(fileId, 0, cachePointer);
    cachePointer.decrementReadersReferrer();

    // Create a copy.

    writeBuffer.flush();
    final Path copyPath = Files.createTempFile("ReadWriteDiskCacheTest", "testFileContentReplacement");
    Files.copy(path, copyPath, StandardCopyOption.REPLACE_EXISTING);

    // Truncate the file.

    writeBuffer.truncateFile(fileId);
    writeBuffer.flush(); // just in case
    Assert.assertTrue(Files.size(path) < Files.size(copyPath));

    // Replace the file content back.

    writeBuffer.replaceFileContentWith(fileId, copyPath);
    Files.delete(copyPath); // cleanup

    // Verify the content.

    final OCachePointer verificationCachePointer = writeBuffer.load(fileId, 0, 1, new OModifiableBoolean(), true)[0];
    verificationCachePointer.acquireSharedLock();
    final Random verificationRandom = new Random(seed);
    final ByteBuffer verificationBuffer = verificationCachePointer.getBufferDuplicate();
    assert verificationBuffer != null;
    Assert.assertTrue(verificationBuffer.limit() > systemOffset);
    for (int i = systemOffset; i < verificationBuffer.limit(); ++i)
      Assert.assertEquals("at " + i, (byte) verificationRandom.nextInt(), verificationBuffer.get(i));
    verificationCachePointer.releaseSharedLock();
    verificationCachePointer.decrementReadersReferrer();
  }

  @Test
  public void testFlushData() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; ++j) {
        entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
        if (entries[i] == null) {
          entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
          Assert.assertEquals(entries[i].getPageIndex(), i);
        }

        final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
        assert buffer != null;
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

    final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 0; i < 4; i++) {
      final OPointer pointer = entries[i].getCachePointer().getPointer();
      OCacheEntry entry = generateEntry(fileId, i, pointer, bufferPool);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 4);

    writeBuffer.flush(fileId);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 3, (byte) i }, new OLogSequenceNumber(0, 0), nativeFileName);
    }

  }

  @Test
  @Ignore
  public void testIfNotEnoughSpaceOldPagesShouldBeMovedToA1Out() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);
    final String nativeFileName = writeBuffer.nativeFileNameById(fileId);

    OCacheEntry[] entries = new OCacheEntry[6];

    for (int i = 0; i < 6; i++) {
      entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
      if (entries[i] == null) {
        entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
        Assert.assertEquals(entries[i].getPageIndex(), i);
      }

      final ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
      assert buffer != null;
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

    final OByteBufferPool bufferPool = OByteBufferPool.instance(null);
    for (int i = 2; i < 6; i++) {
      OCacheEntry entry = generateEntry(fileId, i, entries[i].getCachePointer().getPointer(), bufferPool);
      Assert.assertEquals(a1in.get(entry.getFileId(), entry.getPageIndex()), entry);
    }

    Assert.assertEquals(writeBuffer.getFilledUpTo(fileId), 6);
    writeBuffer.flush();

    for (int i = 0; i < 6; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, new OLogSequenceNumber(0, 0), nativeFileName);
    }
  }

  @Test(expected = OAllCacheEntriesAreUsedException.class)
  public void testIfAllPagesAreUsedExceptionShouldBeThrown() throws Exception {
    long fileId = readBuffer.addFile(fileName, writeBuffer);

    OCacheEntry[] entries = new OCacheEntry[5];
    try {
      for (int i = 0; i < 5; i++) {
        entries[i] = readBuffer.loadForWrite(fileId, i, false, writeBuffer, 1, true, null);
        if (entries[i] == null) {
          entries[i] = readBuffer.allocateNewPage(fileId, writeBuffer, null);
          Assert.assertEquals(entries[i].getPageIndex(), i);
        }

        ByteBuffer buffer = entries[i].getCachePointer().getBufferDuplicate();
        assert buffer != null;
        buffer.position(systemOffset);
        buffer.put(new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 });

        if (i - 4 >= 0) {
          readBuffer.loadForWrite(fileId, i - 4, false, writeBuffer, 0, true, null);

          buffer = entries[i - 4].getCachePointer().getBufferDuplicate();
          assert buffer != null;
          buffer.position(systemOffset);
          buffer.put(new byte[] { (byte) (i - 4), 1, 2, seed, 4, 5, 6, 7 });
        }
      }
    } finally {
      for (int i = 0; i < 4; i++) {
        assert entries[i] != null;
        readBuffer.releaseFromWrite(entries[i], writeBuffer);
      }
    }
  }

  private void assertFile(long pageIndex, byte[] value, OLogSequenceNumber lsn, String fileName) throws IOException {
    OFileClassic fileClassic = new OFileClassic(storagePath.resolve(fileName));
    fileClassic.open();
    byte[] content = new byte[userDataSize + systemOffset];
    final ByteBuffer byteBuffer = ByteBuffer.wrap(content);

    fileClassic.read(pageIndex * (userDataSize + systemOffset), byteBuffer, true);

    Assertions.assertThat(Arrays.copyOfRange(content, systemOffset, userDataSize + systemOffset)).isEqualTo(value);

    long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, 0);
    Assert.assertEquals(magicNumber, OWOWCache.MAGIC_NUMBER_WITH_CHECKSUM);

    long segment = OLongSerializer.INSTANCE.deserializeNative(content, ODurablePage.WAL_SEGMENT_OFFSET);
    long position = OLongSerializer.INSTANCE.deserializeNative(content, ODurablePage.WAL_POSITION_OFFSET);

    OLogSequenceNumber readLsn = new OLogSequenceNumber(segment, position);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

  private OCacheEntry generateEntry(long fileId, long pageIndex, OPointer pointer, OByteBufferPool bufferPool) {
    return new OCacheEntryImpl(fileId, pageIndex, new OCachePointer(pointer, bufferPool, fileId, pageIndex));
  }

  private OCacheEntry generateRemovedEntry(long fileId, long pageIndex) {
    return new OCacheEntryImpl(fileId, pageIndex, null);
  }

  private void setLsn(ByteBuffer buffer, OLogSequenceNumber lsn) {
    buffer.position(OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

    buffer.putLong(lsn.getSegment());
    buffer.putLong(lsn.getPosition());
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[] data;

    @SuppressWarnings("unused")
    public TestRecord() {
    }

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
    public boolean isUpdateMasterRecord() {
      return false;
    }

    @Override
    public byte getId() {
      return (byte) 128;
    }
  }

}
