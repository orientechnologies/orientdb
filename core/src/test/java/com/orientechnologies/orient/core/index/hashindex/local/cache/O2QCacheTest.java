package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.zip.CRC32;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.exception.OAllLRUListEntriesAreUsed;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.ODirtyPagesRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWriteAheadLog;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WriteAheadLogTest;

@Test
public class O2QCacheTest {
  private int                    systemOffset = 2 * (OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE);

  private O2QCache               buffer;
  private OLocalPaginatedStorage storageLocal;
  private ODirectMemory          directMemory;
  private String                 fileName;
  private byte                   seed;
  private OWriteAheadLog         writeAheadLog;

  @BeforeClass
  public void beforeClass() throws IOException {

    OGlobalConfiguration.FILE_LOCK.setValue(Boolean.FALSE);
    directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OLocalPaginatedStorage) Orient.instance().loadStorage("plocal:" + buildDirectory + "/O2QCacheTest");

    fileName = "o2QCacheTest.tst";

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, WriteAheadLogTest.TestRecord.class);
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    closeBufferAndDeleteFile();

    initBuffer();

    Random random = new Random();
    seed = (byte) (random.nextInt() & 0xFF);
  }

  private void closeBufferAndDeleteFile() throws IOException {
    if (buffer != null) {
      buffer.close();
      buffer = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst");
    if (file.exists()) {
      boolean delete = file.delete();
      Assert.assertTrue(delete);
    }
  }

  @AfterClass
  public void afterClass() throws IOException {
    if (buffer != null) {
      buffer.close();
      buffer = null;
    }

    if (writeAheadLog != null) {
      writeAheadLog.delete();
      writeAheadLog = null;
    }

    storageLocal.delete();

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst");
    if (file.exists()) {
      Assert.assertTrue(file.delete());
      file.getParentFile().delete();
    }

  }

  private void initBuffer() throws IOException {
    buffer = new O2QCache(4 * (8 + systemOffset), 15000, directMemory, null, 8 + systemOffset, storageLocal, true);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;
  }

  public void testAddFourItems() throws IOException {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 0, 8);
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateDirtyEntry(fileId, i, pointers[i], new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.flushBuffer();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(0, 0));
    }
  }

  public void testFrequentlyReadItemsAreMovedInAm() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[10];

    for (int i = 0; i < 10; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 0, 8);
      setLsn(pointers[i], new OLogSequenceNumber(1, i));
      buffer.release(fileId, i);
    }

    buffer.clear();

    for (int i = 0; i < 10; i++)
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i));

    for (int i = 0; i < 8; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.release(fileId, i);
    }

    for (int i = 2; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 2);
    Assert.assertEquals(a1in.size(), 2);
    Assert.assertEquals(a1out.size(), 2);

    for (int i = 2; i < 4; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, pointers[i], false, new OLogSequenceNumber(1, i));
      Assert.assertEquals(am.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, ODirectMemory.NULL_POINTER, false, null);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 6; i < 8; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, pointers[i], false, new OLogSequenceNumber(1, i));
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }
  }

  public void testCacheShouldCreateFileIfItIsNotExisted() throws Exception {
    buffer.openFile(fileName);

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst");

    Assert.assertTrue(file.exists());
    Assert.assertTrue(file.isFile());
  }

  public void testFrequentlyAddItemsAreMovedInAm() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[10];

    for (int i = 0; i < 10; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 0, 8);
      setLsn(pointers[i], new OLogSequenceNumber(1, i));
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(a1in.size(), 4);
    Assert.assertEquals(a1out.size(), 2);
    Assert.assertEquals(am.size(), 0);

    for (int i = 6; i < 10; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, pointers[i], true, new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, ODirectMemory.NULL_POINTER, false, null);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 4; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.release(fileId, i);
    }

    Assert.assertEquals(am.size(), 2);
    Assert.assertEquals(a1in.size(), 2);
    Assert.assertEquals(a1out.size(), 2);

    for (int i = 4; i < 6; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, pointers[i], true, new OLogSequenceNumber(0, 0));
      Assert.assertEquals(am.get(fileId, i), lruEntry);
    }

    for (int i = 6; i < 8; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, ODirectMemory.NULL_POINTER, false, null);
      Assert.assertEquals(a1out.get(fileId, i), lruEntry);
    }

    for (int i = 8; i < 10; i++) {
      LRUEntry lruEntry = generateEntry(fileId, i, pointers[i], true, new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(fileId, i), lruEntry);
    }

    buffer.flushBuffer();

    for (int i = 0; i < 10; i++)
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i));

  }

  public void testReadFourItems() throws IOException {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 0, 8);
      setLsn(pointers[i], new OLogSequenceNumber(1, i));
      buffer.release(fileId, i);
    }

    buffer.clear();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(1, i));
    }

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i], false, new OLogSequenceNumber(1, i));
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
  }

  public void testLoadAndLockForReadShouldHitCache() throws Exception {
    long fileId = buffer.openFile(fileName);

    long pointer = buffer.load(fileId, 0);
    buffer.release(fileId, 0);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    LRUEntry entry = generateEntry(fileId, 0, pointer, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
  }

  public void testFlushFileShouldClearDirtyPagesFlag() throws Exception {
    long fileId = buffer.openFile(fileName);
    byte[] value = { (byte) 0, 1, 2, seed, 4, 5, 3, (byte) 0 };

    long pointer = buffer.load(fileId, 0);
    buffer.markDirty(fileId, 0);

    directMemory.set(pointer + systemOffset, value, 0, 8);
    buffer.release(fileId, 0);

    Assert.assertFalse(pointer == ODirectMemory.NULL_POINTER);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    buffer.flushFile(fileId);

    LRUEntry entry = generateEntry(fileId, 0, pointer, false, new OLogSequenceNumber(0, 0));

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
  }

  public void testCloseFileShouldFlushData() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 0, 8);
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateDirtyEntry(fileId, i, pointers[i], new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.closeFile(fileId);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, new OLogSequenceNumber(0, 0));
    }
  }

  public void testCloseFileShouldRemoveFilePagesFromBuffer() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);

      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 0, 8);
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateDirtyEntry(fileId, i, pointers[i], new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.closeFile(fileId);

    Assert.assertEquals(buffer.getA1out().size(), 0);
    Assert.assertEquals(buffer.getA1in().size(), 0);
    Assert.assertEquals(buffer.getAm().size(), 0);
  }

  public void testDeleteFileShouldDeleteFileFromHardDrive() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[4];

    byte[][] content = new byte[4][];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      content[i] = directMemory.get(pointers[i] + systemOffset, 8);
      buffer.release(fileId, i);
    }

    buffer.deleteFile(fileId);
    buffer.flushBuffer();

    for (int i = 0; i < 4; i++) {
      File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst");
      Assert.assertFalse(file.exists());
    }
  }

  public void testFlushData() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; ++j) {
        pointers[i] = buffer.load(fileId, i);
        buffer.markDirty(fileId, i);

        directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, (byte) j, (byte) i }, 0, 8);
        buffer.release(fileId, i);
      }
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateDirtyEntry(fileId, i, pointers[i], new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);

    buffer.flushFile(fileId);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 3, (byte) i }, new OLogSequenceNumber(0, 0));
    }

  }

  public void testIfNotEnoughSpaceOldPagesShouldBeMovedToA1Out() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      buffer.release(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);

    for (int i = 0; i < 2; i++) {
      LRUEntry entry = generateEntry(fileId, i, ODirectMemory.NULL_POINTER, false, null);
      Assert.assertEquals(a1out.get(entry.fileId, entry.pageIndex), entry);
    }

    for (int i = 2; i < 6; i++) {
      LRUEntry entry = generateDirtyEntry(fileId, i, pointers[i], new OLogSequenceNumber(0, 0));
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 6);
    buffer.flushBuffer();

    for (int i = 0; i < 6; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, new OLogSequenceNumber(0, 0));
    }
  }

  public void testIfAllPagesAreUsedInA1InCacheSizeShouldBeIncreased() throws Exception {
    OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.setValue(true);
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[5];

    for (int i = 0; i < 5; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      if (i - 4 >= 0) {
        buffer.load(fileId, i - 4);
        directMemory.set(pointers[i - 4] + systemOffset, new byte[] { (byte) (i - 4), 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      }
    }

    for (int i = 0; i < 5; i++) {
      buffer.release(fileId, i);
      if (i - 4 >= 0) {
        buffer.release(fileId, i - 4);
      }
    }

    int maxSize = buffer.getMaxSize();
    Assert.assertEquals(maxSize, 5);
    OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.setValue(false);
  }

  public void testIfAllPagesAreUsedInAmCacheSizeShouldBeIncreased() throws Exception {
    OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.setValue(true);
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[20];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      buffer.release(fileId, i);
    }

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
    }

    for (int i = 0; i < 4; i++) {
      buffer.release(fileId, i);
    }

    int maxSize = buffer.getMaxSize();
    Assert.assertEquals(maxSize, 5);
    OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.setValue(false);
  }

  @Test(expectedExceptions = OAllLRUListEntriesAreUsed.class)
  public void testIfAllPagesAreUsedExceptionShouldBeThrown() throws Exception {
    OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.setValue(false);
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[5];
    try {
      for (int i = 0; i < 5; i++) {
        pointers[i] = buffer.load(fileId, i);
        buffer.markDirty(fileId, i);
        directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
        if (i - 4 >= 0) {
          buffer.load(fileId, i - 4);
          directMemory.set(pointers[i - 4] + systemOffset, new byte[] { (byte) (i - 4), 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
        }
      }
    } finally {
      for (int i = 0; i < 4; i++) {
        buffer.release(fileId, i);
      }
      OGlobalConfiguration.SERVER_CACHE_2Q_INCREASE_ON_DEMAND.setValue(true);
    }
  }

  public void testDataVerificationOK() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      buffer.release(fileId, i);
    }

    Assert.assertTrue(buffer.checkStoredPages(null).length == 0);
  }

  public void testMagicNumberIsBroken() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      buffer.release(fileId, i);
    }

    buffer.flushBuffer();

    byte[] brokenMagicNumber = new byte[OIntegerSerializer.INT_SIZE];
    OIntegerSerializer.INSTANCE.serializeNative(23, brokenMagicNumber, 0);

    updateFilePage(2, 0, brokenMagicNumber);
    updateFilePage(4, 0, brokenMagicNumber);

    OPageDataVerificationError[] pageErrors = buffer.checkStoredPages(null);
    Assert.assertEquals(2, pageErrors.length);

    Assert.assertTrue(pageErrors[0].incorrectMagicNumber);
    Assert.assertFalse(pageErrors[0].incorrectCheckSum);
    Assert.assertEquals(2, pageErrors[0].pageIndex);
    Assert.assertEquals("o2QCacheTest.tst", pageErrors[0].fileName);

    Assert.assertTrue(pageErrors[1].incorrectMagicNumber);
    Assert.assertFalse(pageErrors[1].incorrectCheckSum);
    Assert.assertEquals(4, pageErrors[1].pageIndex);
    Assert.assertEquals("o2QCacheTest.tst", pageErrors[1].fileName);
  }

  public void testCheckSumIsBroken() throws Exception {
    long fileId = buffer.openFile(fileName);

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 0, 8);
      buffer.release(fileId, i);
    }

    buffer.flushBuffer();

    byte[] brokenByte = new byte[1];
    brokenByte[0] = 13;

    updateFilePage(2, systemOffset + 2, brokenByte);
    updateFilePage(4, systemOffset + 3, brokenByte);

    OPageDataVerificationError[] pageErrors = buffer.checkStoredPages(null);
    Assert.assertEquals(2, pageErrors.length);

    Assert.assertFalse(pageErrors[0].incorrectMagicNumber);
    Assert.assertTrue(pageErrors[0].incorrectCheckSum);
    Assert.assertEquals(2, pageErrors[0].pageIndex);
    Assert.assertEquals("o2QCacheTest.tst", pageErrors[0].fileName);

    Assert.assertFalse(pageErrors[1].incorrectMagicNumber);
    Assert.assertTrue(pageErrors[1].incorrectCheckSum);
    Assert.assertEquals(4, pageErrors[1].pageIndex);
    Assert.assertEquals("o2QCacheTest.tst", pageErrors[1].fileName);
  }

  public void testFlushTillLSN() throws Exception {
    closeBufferAndDeleteFile();

    File file = new File(storageLocal.getConfiguration().getDirectory());
    if (!file.exists())
      file.mkdir();

    writeAheadLog = new OWriteAheadLog(1024, -1, 10 * 1024, 100L * 1024 * 1024 * 1024, storageLocal);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;

    buffer = new O2QCache(4 * (8 + systemOffset), 2, directMemory, writeAheadLog, 8 + systemOffset, storageLocal, true);

    long fileId = buffer.openFile(fileName);
    OLogSequenceNumber lsnToFlush = null;
    for (int i = 0; i < 8; i++) {
      long dataPointer = buffer.load(fileId, i);
      OLogSequenceNumber pageLSN = writeAheadLog.log(new WriteAheadLogTest.TestRecord(30, false));

      setLsn(dataPointer, pageLSN);

      if (i == 5)
        lsnToFlush = pageLSN;

      buffer.markDirty(fileId, i);
      buffer.release(fileId, i);
    }

    Assert.assertEquals(writeAheadLog.getFlushedLSN(), lsnToFlush);
  }

  public void testLogDirtyTables() throws Exception {
    closeBufferAndDeleteFile();

    File file = new File(storageLocal.getConfiguration().getDirectory());
    if (!file.exists())
      file.mkdir();

    writeAheadLog = new OWriteAheadLog(1024, -1, 10 * 1024, 100L * 1024 * 1024 * 1024, storageLocal);
    writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber pageLSN = writeAheadLog.logFuzzyCheckPointEnd();

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;

    buffer = new O2QCache(4 * (8 + systemOffset), 2, directMemory, writeAheadLog, 8 + systemOffset, storageLocal, true);

    long fileId = buffer.openFile(fileName);
    for (int i = 0; i < 8; i++) {
      long dataPointer = buffer.load(fileId, i);
      setLsn(dataPointer, pageLSN);

      buffer.markDirty(fileId, i);
      buffer.release(fileId, i);
    }

    buffer.flushBuffer();
    buffer.clear();

    writeAheadLog.getFlushedLSN();

    writeAheadLog.logFuzzyCheckPointStart();
    OLogSequenceNumber lsn = writeAheadLog.logFuzzyCheckPointEnd();

    for (int i = 0; i < 8; i++) {
      long dataPointer = buffer.load(fileId, i);
      setLsn(dataPointer, lsn);

      buffer.markDirty(fileId, i);
      buffer.release(fileId, i);
    }

    Set<ODirtyPage> dirtyPages = buffer.logDirtyPagesTable();
    Set<ODirtyPage> expectedDirtyPages = new HashSet<ODirtyPage>();
    for (int i = 7; i >= 2; i--)
      expectedDirtyPages.add(new ODirtyPage("o2QCacheTest.tst", i, pageLSN));

    Assert.assertEquals(dirtyPages, expectedDirtyPages);

    lsn = writeAheadLog.begin();
    OLogSequenceNumber prevLSN = null;
    while (lsn != null) {
      prevLSN = lsn;
      lsn = writeAheadLog.next(lsn);
    }

    final ODirtyPagesRecord dirtyPagesRecord = (ODirtyPagesRecord) writeAheadLog.read(prevLSN);
    Assert.assertEquals(dirtyPagesRecord.getDirtyPages(), dirtyPages);
  }

  private void updateFilePage(long pageIndex, long offset, byte[] value) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst";

    OFileClassic fileClassic = new OFileClassic();
    fileClassic.init(path, "rw");
    fileClassic.open();

    fileClassic.write(pageIndex * (8 + systemOffset) + offset, value, value.length, 0);

    fileClassic.close();
  }

  private void assertFile(long pageIndex, byte[] value, OLogSequenceNumber lsn) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.tst";

    OFileClassic fileClassic = new OFileClassic();
    fileClassic.init(path, "r");
    fileClassic.open();
    byte[] content = new byte[8 + systemOffset];
    fileClassic.read(pageIndex * (8 + systemOffset), content, 8 + systemOffset);

    Assert.assertEquals(Arrays.copyOfRange(content, systemOffset, 8 + systemOffset), value);

    long magicNumber = OLongSerializer.INSTANCE.deserializeNative(content, 0);

    Assert.assertEquals(magicNumber, O2QCache.MAGIC_NUMBER);
    CRC32 crc32 = new CRC32();
    crc32.update(content, OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE, content.length - OIntegerSerializer.INT_SIZE
        - OLongSerializer.LONG_SIZE);

    int crc = OIntegerSerializer.INSTANCE.deserializeNative(content, OLongSerializer.LONG_SIZE);
    Assert.assertEquals(crc, (int) crc32.getValue());

    int segment = OIntegerSerializer.INSTANCE.deserializeNative(content, OLongSerializer.LONG_SIZE + OIntegerSerializer.INT_SIZE);
    long position = OLongSerializer.INSTANCE
        .deserializeNative(content, OLongSerializer.LONG_SIZE + 2 * OIntegerSerializer.INT_SIZE);

    OLogSequenceNumber readLsn = new OLogSequenceNumber(segment, position);

    Assert.assertEquals(readLsn, lsn);

    fileClassic.close();
  }

  private LRUEntry generateDirtyEntry(long fileId, long pageIndex, long pointer, OLogSequenceNumber lsn) {
    return generateEntry(fileId, pageIndex, pointer, true, lsn);
  }

  private LRUEntry generateEntry(long fileId, long pageIndex, long pointer, boolean dirty, OLogSequenceNumber lsn) {
    LRUEntry entry = new LRUEntry();
    entry.fileId = fileId;
    entry.pageIndex = pageIndex;
    entry.dataPointer = pointer;
    entry.isDirty = dirty;
    entry.loadedLSN = lsn;
    return entry;
  }

  private void setLsn(long dataPointer, OLogSequenceNumber lsn) {
    ODirectMemory directMemory = ODirectMemoryFactory.INSTANCE.directMemory();
    OIntegerSerializer.INSTANCE.serializeInDirectMemory(lsn.getSegment(), directMemory, dataPointer + OIntegerSerializer.INT_SIZE
        + OLongSerializer.LONG_SIZE);
    OLongSerializer.INSTANCE.serializeInDirectMemory(lsn.getPosition(), directMemory, dataPointer + 2 * OIntegerSerializer.INT_SIZE
        + OLongSerializer.LONG_SIZE);
  }
}
