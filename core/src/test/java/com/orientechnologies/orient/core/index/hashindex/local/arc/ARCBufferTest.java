package com.orientechnologies.orient.core.index.hashindex.local.arc;

import java.io.File;
import java.io.IOException;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * @author Andrey Lomakin
 * @since 26.02.13
 */
@Test
public class ARCBufferTest {
  private OARCBuffer    buffer;
  private OStorageLocal storageLocal;
  private ODirectMemory directMemory;

  @BeforeClass
  public void beforeClass() throws IOException {
    directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OStorageLocal) Orient.instance().loadStorage("local:" + buildDirectory + "/ARCBufferTest");
  }

  private void initBuffer() throws IOException {
    buffer = new OARCBuffer(32, directMemory, 8, storageLocal, true);

    final OStorageSegmentConfiguration segmentConfiguration = new OStorageSegmentConfiguration(storageLocal.getConfiguration(),
        "arcBufferTest", 0);
    segmentConfiguration.fileType = OFileFactory.CLASSIC;

    buffer.openFile(segmentConfiguration, ".tst");
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    if (buffer != null) {
      buffer.close();

      File file = new File(storageLocal.getConfiguration().getDirectory() + "/arcBufferTest.0.tst");
      if (file.exists())
        file.delete();
    }

    initBuffer();
  }

  @AfterClass
  public void afterClass() throws IOException {
    buffer.close();

    File file = new File(storageLocal.getConfiguration().getDirectory() + "/arcBufferTest.0.tst");
    if (file.exists())
      file.delete();

    storageLocal.delete();
  }

  public void testAddOneItem() throws IOException {
    long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 0);
    directMemory.set(pagePointer, new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 0);
    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 0));
    Assert.assertEquals(buffer.getFilledUpTo("arcBufferTest", ".tst"), 1);

    buffer.flush();

    assertFile(0, new byte[] { 0, 1, 2, 3, 4, 5, 6, 7 });
  }

  public void testAddFourItems() throws IOException {
    for (int i = 0; i < 4; i++) {
      long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", i);
      directMemory.set(pagePointer, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock("arcBufferTest", ".tst", i);
    }

    for (int i = 0; i < 4; i++)
      Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", i));

    Assert.assertEquals(buffer.getFilledUpTo("arcBufferTest", ".tst"), 4);
    buffer.flush();

    for (int i = 0; i < 4; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i });
    }

  }

  public void testAddFiveItems() throws IOException {
    for (int i = 0; i < 5; i++) {
      long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", i);
      directMemory.set(pagePointer, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock("arcBufferTest", ".tst", i);
    }

    assertFile(0, new byte[] { 0, 1, 2, 3, 4, 5, 6, 0 });

    for (int i = 1; i < 5; i++)
      Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", i));

    Assert.assertEquals(buffer.getFilledUpTo("arcBufferTest", ".tst"), 5);
    buffer.flush();

    for (int i = 0; i < 5; i++) {
      assertFile(i, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i });
    }
  }

  public void testAddFourItemsTwoSecondTime() throws IOException {
    for (int i = 0; i < 4; i++) {
      long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", i);
      directMemory.set(pagePointer, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock("arcBufferTest", ".tst", i);
    }

    long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 1);
    directMemory.set(pagePointer, new byte[] { (byte) 10, 1, 2, 3, 4, 5, 6, (byte) 10 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 1);

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 3);
    directMemory.set(pagePointer, new byte[] { (byte) 30, 1, 2, 3, 4, 5, 6, (byte) 30 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 3);

    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 0));
    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 2));

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 1));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 3));

    Assert.assertEquals(buffer.getFilledUpTo("arcBufferTest", ".tst"), 4);
    buffer.flush();

    assertFile(0, new byte[] { (byte) 0, 1, 2, 3, 4, 5, 6, (byte) 0 });
    assertFile(1, new byte[] { (byte) 10, 1, 2, 3, 4, 5, 6, (byte) 10 });
    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });
    assertFile(3, new byte[] { (byte) 30, 1, 2, 3, 4, 5, 6, (byte) 30 });
  }

  public void testAddFourItemsOneThreeTimes() throws IOException {
    for (int i = 0; i < 4; i++) {
      long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", i);
      directMemory.set(pagePointer, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock("arcBufferTest", ".tst", i);
    }

    long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 1);
    directMemory.set(pagePointer, new byte[] { (byte) 10, 1, 2, 3, 4, 5, 6, (byte) 10 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 1);

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 1);
    directMemory.set(pagePointer, new byte[] { (byte) 40, 1, 2, 3, 4, 5, 6, (byte) 40 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 1);

    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 0));
    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 2));
    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 3));

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 1));

    Assert.assertEquals(buffer.getFilledUpTo("arcBufferTest", ".tst"), 4);
    buffer.flush();

    assertFile(0, new byte[] { (byte) 0, 1, 2, 3, 4, 5, 6, (byte) 0 });
    assertFile(1, new byte[] { (byte) 40, 1, 2, 3, 4, 5, 6, (byte) 40 });
    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });
    assertFile(3, new byte[] { (byte) 3, 1, 2, 3, 4, 5, 6, (byte) 3 });
  }

  public void testSingleEvictedItemMovedToMultiEvicted() throws IOException {
    Assert.assertEquals(buffer.minSingleLRUSize(), 0);

    for (int i = 0; i < 4; i++) {
      long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", i);
      directMemory.set(pagePointer, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock("arcBufferTest", ".tst", i);
    }

    long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 0);
    directMemory.set(pagePointer, new byte[] { (byte) 100, 1, 2, 3, 4, 5, 6, (byte) 100 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 0);

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 4);
    directMemory.set(pagePointer, new byte[] { (byte) 121, 1, 2, 3, 4, 5, 6, (byte) 121 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 4);

    assertFile(1, new byte[] { (byte) 1, 1, 2, 3, 4, 5, 6, (byte) 1 });

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 1);
    directMemory.set(pagePointer, new byte[] { (byte) 132, 1, 2, 3, 4, 5, 6, (byte) 132 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 1);

    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });

    Assert.assertEquals(buffer.minSingleLRUSize(), 1);

    buffer.flush();

    assertFile(0, new byte[] { (byte) 100, 1, 2, 3, 4, 5, 6, (byte) 100 });
    assertFile(1, new byte[] { (byte) 132, 1, 2, 3, 4, 5, 6, (byte) 132 });
    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });
    assertFile(3, new byte[] { (byte) 3, 1, 2, 3, 4, 5, 6, (byte) 3 });
    assertFile(4, new byte[] { (byte) 121, 1, 2, 3, 4, 5, 6, (byte) 121 });
  }

  public void testMultiEvictedItemMovedToMultiEvicted() throws IOException {
    Assert.assertEquals(buffer.minSingleLRUSize(), 0);

    // all in first LRU
    for (int i = 0; i < 4; i++) {
      long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", i);
      directMemory.set(pagePointer, new byte[] { (byte) i, 1, 2, 3, 4, 5, 6, (byte) i }, 8);
      buffer.releaseWriteLock("arcBufferTest", ".tst", i);
    }

    for (int i = 0; i < 4; i++)
      Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", i));

    // 0 in several LRU
    long pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 0);
    directMemory.set(pagePointer, new byte[] { (byte) 100, 1, 2, 3, 4, 5, 6, (byte) 100 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 0);

    for (int i = 1; i < 4; i++)
      Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", i));

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 0));

    // 1 in evicted
    // 4 in single LRU
    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 4);
    directMemory.set(pagePointer, new byte[] { (byte) 121, 1, 2, 3, 4, 5, 6, (byte) 121 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 4);

    for (int i = 2; i <= 4; i++)
      Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", i));

    Assert.assertTrue(buffer.inFetchedOnceEvictedLRU("arcBufferTest.tst", 1));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 0));

    assertFile(1, new byte[] { (byte) 1, 1, 2, 3, 4, 5, 6, (byte) 1 });

    // 1 in several LRU
    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 1);
    directMemory.set(pagePointer, new byte[] { (byte) 132, 1, 2, 3, 4, 5, 6, (byte) 132 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 1);

    for (int i = 3; i <= 4; i++)
      Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", i));

    Assert.assertTrue(buffer.inFetchedOnceEvictedLRU("arcBufferTest.tst", 2));

    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 1));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 0));

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 4);
    directMemory.set(pagePointer, new byte[] { (byte) 135, 1, 2, 3, 4, 5, 6, (byte) 135 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 4);

    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 3));

    Assert.assertTrue(buffer.inFetchedOnceEvictedLRU("arcBufferTest.tst", 2));

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 4));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 1));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 0));

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 5);
    directMemory.set(pagePointer, new byte[] { (byte) 105, 1, 2, 3, 4, 5, 6, (byte) 105 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 5);

    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 5));
    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 3));

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 4));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 1));

    Assert.assertTrue(buffer.inFetchedOnceEvictedLRU("arcBufferTest.tst", 2));
    Assert.assertTrue(buffer.inFetchedSeveralTimesEvictedLRU("arcBufferTest.tst", 0));

    assertFile(0, new byte[] { (byte) 100, 1, 2, 3, 4, 5, 6, (byte) 100 });

    Assert.assertEquals(buffer.minSingleLRUSize(), 1);

    pagePointer = buffer.loadAndLockForWrite("arcBufferTest", ".tst", 0);
    directMemory.set(pagePointer, new byte[] { (byte) 11, 1, 2, 3, 4, 5, 6, (byte) 11 }, 8);
    buffer.releaseWriteLock("arcBufferTest", ".tst", 0);

    Assert.assertTrue(buffer.inFetchedOnceLRU("arcBufferTest.tst", 5));

    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 0));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 4));
    Assert.assertTrue(buffer.inFetchedSeveralTimesLRU("arcBufferTest.tst", 1));

    Assert.assertTrue(buffer.inFetchedOnceEvictedLRU("arcBufferTest.tst", 2));
    Assert.assertTrue(buffer.inFetchedOnceEvictedLRU("arcBufferTest.tst", 3));

    Assert.assertEquals(buffer.minSingleLRUSize(), 0);

    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });
    assertFile(3, new byte[] { (byte) 3, 1, 2, 3, 4, 5, 6, (byte) 3 });

    Assert.assertEquals(buffer.getFilledUpTo("arcBufferTest", ".tst"), 6);
    buffer.flush();

    assertFile(0, new byte[] { (byte) 11, 1, 2, 3, 4, 5, 6, (byte) 11 });
    assertFile(1, new byte[] { (byte) 132, 1, 2, 3, 4, 5, 6, (byte) 132 });
    assertFile(2, new byte[] { (byte) 2, 1, 2, 3, 4, 5, 6, (byte) 2 });
    assertFile(3, new byte[] { (byte) 3, 1, 2, 3, 4, 5, 6, (byte) 3 });
    assertFile(4, new byte[] { (byte) 135, 1, 2, 3, 4, 5, 6, (byte) 135 });
    assertFile(5, new byte[] { (byte) 105, 1, 2, 3, 4, 5, 6, (byte) 105 });
  }

  private void assertFile(long pageIndex, byte[] value) throws IOException {
    String path = storageLocal.getConfiguration().getDirectory() + "/arcBufferTest.0.tst";

    OFileClassic fileClassic = new OFileClassic();
    fileClassic.init(path, "r");
    fileClassic.open();
    byte[] content = new byte[8];
    fileClassic.read(pageIndex * 8, content, 8);

    Assert.assertEquals(content, value);
    fileClassic.close();
  }
}
