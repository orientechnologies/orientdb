package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

@Test
public class O2QCacheTest {
  private O2QCache                     buffer;
  private OStorageLocal                storageLocal;
  private ODirectMemory                directMemory;
  private OStorageSegmentConfiguration fileConfiguration;
  private byte                         seed;

  @BeforeClass
  public void beforeClass() throws IOException {
    directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OStorageLocal) Orient.instance().loadStorage("local:" + buildDirectory + "/O2QCacheTest");

    fileConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(), "o2QCacheTest", 0);
    fileConfiguration.fileType = OFileFactory.CLASSIC;
    fileConfiguration.fileMaxSize = "10000Mb";
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    if (buffer != null) {
      buffer.close();

      File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst");
      if (file.exists()) {
        boolean delete = file.delete();
        Assert.assertTrue(delete);
      }
    }

    initBuffer();

    Random random = new Random();
    seed = (byte) (random.nextInt() & 0xFF);
  }

  @AfterClass
  public void afterClass() throws IOException {
    buffer.close();
    storageLocal.delete();

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst");
    if (file.exists())
      Assert.assertTrue(file.delete());
  }

  private void initBuffer() throws IOException {
    buffer = new O2QCache(32, directMemory, 8, storageLocal, true);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;
  }

  public void testAddFourItems() throws IOException {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.loadAndLockForWrite(fileId, i);
      directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i]);
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.flushBuffer();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });
    }
  }

  @Test(enabled = false)
  public void testAllocateAndWrite() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.allocateAndLockForWrite(fileId, i);
      directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i], false, true);
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.flushBuffer();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });
    }

  }

  @Test(enabled = false)
  public void testAllocateShouldEnlargeFileIfNotEnoughSpace() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long pointer = buffer.allocateAndLockForWrite(fileId, 100);
    directMemory.set(pointer, new byte[] { (byte) 0, 1, 2, seed, 4, 5, 6, (byte) 0 }, 8);
    buffer.releaseWriteLock(fileId, 100);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    Assert.assertEquals(a1in.get(fileId, 100), generateEntry(fileId, 100, pointer, false, true));

    assertFile(100, new byte[] { (byte) 0, 1, 2, seed, 4, 5, 6, (byte) 0 });
  }

  @Test
  public void testCacheHitNotExistedPage() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    buffer.cacheHit(fileId, 0, 1234567);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    LRUEntry entry = generateEntry(fileId, 0, 1234567, false, true);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
  }

  @Test
  public void testGetForWriteReturnNullPointerIfPageDoesNotExists() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long pointer = buffer.getAndLockForWrite(fileId, 1);

    Assert.assertEquals(pointer, ODirectMemory.NULL_POINTER);
  }

  @Test
  public void testGetForWriteReturnExistedPage() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");
    byte[] value = { (byte) 0, 1, 2, seed, 4, 5, 3, (byte) 0 };

    long pointer = buffer.loadAndLockForWrite(fileId, 0);
    directMemory.set(pointer, value, 8);
    buffer.releaseWriteLock(fileId, 0);

    Assert.assertFalse(pointer == ODirectMemory.NULL_POINTER);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    LRUEntry entry = generateEntry(fileId, 0, pointer);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);

    long pointerFromCache = buffer.getAndLockForWrite(fileId, 0);
    Assert.assertEquals(pointerFromCache, pointer);
    value[6] = 10;
    directMemory.set(pointerFromCache, value, 8);
    buffer.releaseWriteLock(fileId, 0);

    buffer.flushBuffer();

    assertFile(0, value);
  }

  @Test
  public void testClearExternalManagementFlag() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long pointer = buffer.allocateAndLockForWrite(fileId, 0);
    buffer.releaseWriteLock(fileId, 0);

    buffer.clearExternalManagementFlag(fileId, 0);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    LRUEntry entry = generateEntry(fileId, 0, pointer, false);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
  }

  @Test
  public void testLoadAndLockForReadShouldHitCache() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long pointer = buffer.loadAndLockForRead(fileId, 0);
    buffer.releaseReadLock(fileId, 0);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);
    LRUEntry entry = generateEntry(fileId, 0, pointer, false);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
  }

  @Test
  public void testFlushFileShouldClearDirtyPagesFlag() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");
    byte[] value = { (byte) 0, 1, 2, seed, 4, 5, 3, (byte) 0 };

    long pointer = buffer.loadAndLockForWrite(fileId, 0);
    directMemory.set(pointer, value, 8);
    buffer.releaseWriteLock(fileId, 0);

    Assert.assertFalse(pointer == ODirectMemory.NULL_POINTER);

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    buffer.flushFile(fileId);

    LRUEntry entry = generateEntry(fileId, 0, pointer, false);

    Assert.assertEquals(a1in.size(), 1);
    Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
  }

  @Test
  public void testFreePageShouldRemoveEntryFromBuffer() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    buffer.loadAndLockForWrite(fileId, 0);

    LRUList a1in = buffer.getA1in();

    int size = a1in.size();

    buffer.freePage(fileId, 0);

    Assert.assertEquals(a1in.size(), size - 1);
  }

  @Test
  public void testFreePageShouldRemoveEvictedPage() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    Object value = OGlobalConfiguration.DISK_CACHE_WRITE_QUEUE_LENGTH.getValue();
    try {
      OGlobalConfiguration.DISK_CACHE_WRITE_QUEUE_LENGTH.setValue(16);
      long[] pointers;
      pointers = new long[16];
      byte[][] content = new byte[2][];

      for (int i = 0; i < 2; i++) {
        pointers[i] = buffer.loadAndLockForRead(fileId, i);
        content[i] = directMemory.get(pointers[i], 8);
        buffer.releaseReadLock(fileId, i);
      }

      for (int i = 0; i < 16; i++) {
        pointers[i] = buffer.loadAndLockForWrite(fileId, i);
        directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 8);
        buffer.releaseWriteLock(fileId, i);
      }

      Map evictedPages = buffer.getEvictedPages();

      int size = evictedPages.size();

      buffer.freePage(fileId, 0);

      Assert.assertEquals(evictedPages.size(), size - 1);

      buffer.freePage(fileId, 1);

      Assert.assertEquals(evictedPages.size(), size - 2);

      buffer.flushBuffer();

      for (int i = 0; i < 2; i++) {
        assertFile(i, content[i]);
      }

      for (int i = 2; i < 16; i++) {
        assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 });
      }
    } finally {
      OGlobalConfiguration.DISK_CACHE_WRITE_QUEUE_LENGTH.setValue(value);
    }
  }

  @Test
  public void testCloseFileShouldFlushData() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.loadAndLockForWrite(fileId, i);
      directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i]);
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.closeFile(fileId);

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i });
    }
  }

  @Test
  public void testCloseFileShouldRemoveFilePagesFromBuffer() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.loadAndLockForWrite(fileId, i);
      directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i]);
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);
    buffer.closeFile(fileId);

    Assert.assertEquals(buffer.getA1out().size(), 0);
    Assert.assertEquals(buffer.getA1in().size(), 0);
    Assert.assertEquals(buffer.getAm().size(), 0);
  }

  @Test
  public void testDeleteFileShouldDeleteFileFromHardDrive() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    byte[][] content = new byte[4][];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.loadAndLockForRead(fileId, i);
      content[i] = directMemory.get(pointers[i], 8);
      buffer.releaseReadLock(fileId, i);
    }

    buffer.deleteFile(fileId);
    buffer.flushBuffer();

    for (int i = 0; i < 4; i++) {
      File file = new File(storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst");
      Assert.assertFalse(file.exists());
    }
  }

  @Test
  public void testFlushData() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; ++j) {
        pointers[i] = buffer.loadAndLockForWrite(fileId, i);
        directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, (byte) j, (byte) i }, 8);
        buffer.releaseWriteLock(fileId, i);
      }
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);
    Assert.assertEquals(a1out.size(), 0);

    for (int i = 0; i < 4; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i]);
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 4);

    for (int i = 0; i < 4; i++) {
      buffer.flushData(fileId, i, pointers[i]);
    }

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 3, (byte) i });
    }

  }

  @Test
  public void testIfNotEnoughSpaceOldPagesShouldBeMovedToA1Out() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.loadAndLockForWrite(fileId, i);
      directMemory.set(pointers[i], new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 8);
      buffer.releaseWriteLock(fileId, i);
    }

    LRUList am = buffer.getAm();
    LRUList a1in = buffer.getA1in();
    LRUList a1out = buffer.getA1out();

    Assert.assertEquals(am.size(), 0);

    for (int i = 0; i < 2; i++) {
      LRUEntry entry = generateEntry(fileId, i, ODirectMemory.NULL_POINTER, false);
      Assert.assertEquals(a1out.get(entry.fileId, entry.pageIndex), entry);
    }

    for (int i = 2; i < 6; i++) {
      LRUEntry entry = generateEntry(fileId, i, pointers[i]);
      Assert.assertEquals(a1in.get(entry.fileId, entry.pageIndex), entry);
    }

    Assert.assertEquals(buffer.getFilledUpTo(fileId), 6);
    buffer.flushBuffer();

    for (int i = 0; i < 6; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 });
    }
  }

  private void assertFile(long pageIndex, byte[] value) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst";

    OFileClassic fileClassic = new OFileClassic();
    fileClassic.init(path, "r");
    fileClassic.open();
    byte[] content = new byte[8];
    fileClassic.read(pageIndex * 8, content, 8);

    Assert.assertEquals(content, value);
    fileClassic.close();
  }

  private LRUEntry generateEntry(long fileId, long pageIndex, long pointer) {
    return generateEntry(fileId, pageIndex, pointer, true);
  }

  private LRUEntry generateEntry(long fileId, long pageIndex, long pointer, boolean dirty) {
    return generateEntry(fileId, pageIndex, pointer, dirty, false);
  }

  private LRUEntry generateEntry(long fileId, long pageIndex, long pointer, boolean isDirty, boolean isExternallyManaged) {
    LRUEntry entry = new LRUEntry();
    entry.fileId = fileId;
    entry.pageIndex = pageIndex;
    entry.dataPointer = pointer;
    entry.managedExternally = isExternallyManaged;
    entry.isDirty = isDirty;
    return entry;
  }
}
