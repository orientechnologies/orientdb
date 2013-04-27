package com.orientechnologies.orient.core.index.hashindex.local.cache;

import java.io.File;
import java.io.IOException;
import java.util.Random;

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
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

@Test
public class O2QCacheTest {
  private int                          systemOffset = OIntegerSerializer.INT_SIZE + OLongSerializer.LONG_SIZE;

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
    buffer = new O2QCache(4 * (8 + systemOffset), directMemory, 8 + systemOffset, storageLocal, true);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "o2QCacheTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;
  }

  public void testAddFourItems() throws IOException {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.release(fileId, i);
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

  @Test
  public void testLoadAndLockForReadShouldHitCache() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long pointer = buffer.load(fileId, 0);
    buffer.release(fileId, 0);

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

    long pointer = buffer.load(fileId, 0);
    buffer.markDirty(fileId, 0);

    directMemory.set(pointer + systemOffset, value, 8);
    buffer.release(fileId, 0);

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
  public void testCloseFileShouldFlushData() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.release(fileId, i);
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
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);

      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, (byte) i }, 8);
      buffer.release(fileId, i);
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

  @Test
  public void testFlushData() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[4];

    for (int i = 0; i < 4; i++) {
      for (int j = 0; j < 4; ++j) {
        pointers[i] = buffer.load(fileId, i);
        buffer.markDirty(fileId, i);

        directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, (byte) j, (byte) i }, 8);
        buffer.release(fileId, i);
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

    buffer.flushFile(fileId);

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
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 8);
      buffer.release(fileId, i);
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

  public void testDataVerificationOK() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 8);
      buffer.release(fileId, i);
    }

    Assert.assertTrue(buffer.checkStoredPages(null).length == 0);
  }

  public void testMagicNumberIsBroken() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 8);
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
    Assert.assertEquals("o2QCacheTest", pageErrors[0].fileName);

    Assert.assertTrue(pageErrors[1].incorrectMagicNumber);
    Assert.assertFalse(pageErrors[1].incorrectCheckSum);
    Assert.assertEquals(4, pageErrors[1].pageIndex);
    Assert.assertEquals("o2QCacheTest", pageErrors[1].fileName);
  }

  public void testCheckSumIsBroken() throws Exception {
    long fileId = buffer.openFile(fileConfiguration, ".tst");

    long[] pointers;
    pointers = new long[6];

    for (int i = 0; i < 6; i++) {
      pointers[i] = buffer.load(fileId, i);
      buffer.markDirty(fileId, i);
      directMemory.set(pointers[i] + systemOffset, new byte[] { (byte) i, 1, 2, seed, 4, 5, 6, 7 }, 8);
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
    Assert.assertEquals("o2QCacheTest", pageErrors[0].fileName);

    Assert.assertFalse(pageErrors[1].incorrectMagicNumber);
    Assert.assertTrue(pageErrors[1].incorrectCheckSum);
    Assert.assertEquals(4, pageErrors[1].pageIndex);
    Assert.assertEquals("o2QCacheTest", pageErrors[1].fileName);
  }

  private void updateFilePage(long pageIndex, long offset, byte[] value) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst";

    OFileClassic fileClassic = new OFileClassic();
    fileClassic.init(path, "rw");
    fileClassic.open();

    fileClassic.write(pageIndex * (8 + systemOffset) + offset, value, value.length, 0);

    fileClassic.close();
  }

  private void assertFile(long pageIndex, byte[] value) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest.0.tst";

    OFileClassic fileClassic = new OFileClassic();
    fileClassic.init(path, "r");
    fileClassic.open();
    byte[] content = new byte[8];
    fileClassic.read(pageIndex * (8 + systemOffset) + systemOffset, content, 8);

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
    entry.isDirty = isDirty;
    return entry;
  }
}
