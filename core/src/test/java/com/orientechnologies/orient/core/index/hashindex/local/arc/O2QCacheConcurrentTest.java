package com.orientechnologies.orient.core.index.hashindex.local.arc;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReferenceArray;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.common.directmemory.ODirectMemory;
import com.orientechnologies.common.directmemory.ODirectMemoryFactory;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;
import com.orientechnologies.orient.core.index.hashindex.local.cache.O2QCache;
import com.orientechnologies.orient.core.storage.fs.OFileClassic;
import com.orientechnologies.orient.core.storage.fs.OFileFactory;
import com.orientechnologies.orient.core.storage.impl.local.OStorageLocal;

/**
 * Created with IntelliJ IDEA. User: ALoginov Date: 3/26/13 Time: 1:21 PM To change this template use File | Settings | File
 * Templates.
 */
public class O2QCacheConcurrentTest {

  private static final int                    THREAD_COUNT    = 4;
  private static final int                    PAGE_COUNT      = 20;
  private static final int                    FILE_COUNT      = 8;
  private O2QCache                            buffer;
  private OStorageLocal                       storageLocal;
  private ODirectMemory                       directMemory;
  private OStorageSegmentConfiguration[]      fileConfigurations;
  private byte                                seed;
  private final ExecutorService               executorService = Executors.newFixedThreadPool(THREAD_COUNT);
  private final List<Future<Void>>            futures         = new ArrayList<Future<Void>>(THREAD_COUNT);
  private AtomicLongArray                     fileIds         = new AtomicLongArray(FILE_COUNT);
  private AtomicIntegerArray                  pageCounters    = new AtomicIntegerArray(FILE_COUNT);
  private AtomicReferenceArray<List<Integer>> pagesQueue      = new AtomicReferenceArray<List<Integer>>(FILE_COUNT);

  private AtomicBoolean                       continuousWrite = new AtomicBoolean(true);
  private AtomicInteger                       version         = new AtomicInteger(1);

  @BeforeClass
  public void beforeClass() throws IOException {
    directMemory = ODirectMemoryFactory.INSTANCE.directMemory();

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = ".";

    storageLocal = (OStorageLocal) Orient.instance().loadStorage("local:" + buildDirectory + "/O2QCacheTest");

    fileConfigurations = new OStorageSegmentConfiguration[FILE_COUNT];
    for (int i = 0; i < fileConfigurations.length; i++) {
      fileConfigurations[i] = new OStorageSegmentConfiguration(storageLocal.getConfiguration(), "o2QCacheTest" + i, 0);
      fileConfigurations[i].fileType = OFileFactory.CLASSIC;
      fileConfigurations[i].fileMaxSize = "10000Mb";
    }

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

  @Test
  public void testAdd() throws Exception {
    for (int i = 0; i < fileIds.length(); i++) {
      fileIds.set(i, buffer.openFile(fileConfigurations[i], ".tst"));
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
      futures.add(executorService.submit(new Writer()));
    }

    for (Future<Void> future : futures)
      future.get();

    futures.clear();

    buffer.flushBuffer();

    validate(version.byteValue());

    version.compareAndSet(1, 2);
    continuousWrite.compareAndSet(true, false);

    List<Integer>[] array = new ArrayList[FILE_COUNT];
    for (int k = 0; k < FILE_COUNT; ++k) {
      array[k] = new ArrayList<Integer>(PAGE_COUNT);
      for (Integer i = 0; i < PAGE_COUNT; ++i) {
        array[k].add(i);
      }
    }
    for (int i = 0; i < FILE_COUNT; ++i) {
      pagesQueue.set(i, Collections.synchronizedList(array[i]));
    }

    for (int i = 0; i < THREAD_COUNT; i++) {
      futures.add(executorService.submit(new Writer()));
    }
    for (int i = 0; i < THREAD_COUNT; i++) {
      futures.add(executorService.submit(new Reader()));
    }

    for (Future<Void> future : futures)
      future.get();

    buffer.flushBuffer();

    validate(version.byteValue());
  }

  private void validate(byte version) throws IOException {
    for (int k = 0; k < FILE_COUNT; ++k) {
      String path = storageLocal.getConfiguration().getDirectory() + "/o2QCacheTest" + k + ".0.tst";

      OFileClassic fileClassic = new OFileClassic();
      fileClassic.init(path, "r");
      fileClassic.open();
      for (int i = 0; i < PAGE_COUNT; i++) {
        byte[] content = new byte[8];
        fileClassic.read(i * 8, content, 8);

        Assert.assertEquals(content, new byte[] { version, 2, 3, seed, 5, 6, (byte) k, (byte) (i & 0xFF) }, " i = " + i);
      }
      fileClassic.close();
    }
  }

  private class Writer implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      int fileNumber = getNextFileNumber();
      while (shouldContinue(fileNumber)) {
        final long pageIndex;
        pageIndex = getNextPageIndex(fileNumber);
        long pointer = buffer.loadAndLockForWrite(fileIds.get(fileNumber), pageIndex);

        directMemory.set(pointer,
            new byte[] { version.byteValue(), 2, 3, seed, 5, 6, (byte) fileNumber, (byte) (pageIndex & 0xFF) }, 8);

        buffer.releaseWriteLock(fileIds.get(fileNumber), pageIndex);
        fileNumber = getNextFileNumber();
      }
      return null;
    }

    private long getNextPageIndex(int fileNumber) {
      long pageIndex;
      if (continuousWrite.get()) {
        pageIndex = pageCounters.getAndIncrement(fileNumber);
      } else {
        pageIndex = pagesQueue.get(fileNumber).remove(new Random().nextInt(pagesQueue.get(fileNumber).size()));
      }
      return pageIndex;
    }

    private boolean shouldContinue(int fileNumber) {
      return fileNumber != -1;
    }

    public int getNextFileNumber() {
      if (continuousWrite.get()) {
        int result = new Random().nextInt(FILE_COUNT - 1);
        for (int i = 0; i < FILE_COUNT; ++i) {
          if (pageCounters.get((result + i) % FILE_COUNT) < PAGE_COUNT) {
            return (result + i) % FILE_COUNT;
          }
        }
      } else {
        int result = new Random().nextInt(FILE_COUNT - 1);
        for (int i = 0; i < FILE_COUNT; ++i) {
          if (!pagesQueue.get((result + i) % FILE_COUNT).isEmpty()) {
            return (result + i) % FILE_COUNT;
          }
        }
      }
      return -1;
    }
  }

  private class Reader implements Callable<Void> {
    @Override
    public Void call() throws Exception {
      long pageIndex = Math.abs(new Random().nextInt() % PAGE_COUNT);
      int fileNumber = new Random().nextInt(FILE_COUNT);

      long pointer = buffer.loadAndLockForRead(fileIds.get(fileNumber), pageIndex);

      byte[] content = directMemory.get(pointer, 8);

      buffer.releaseReadLock(fileIds.get(fileNumber), pageIndex);

      Assert.assertTrue(content[0] == 1 || content[0] == 2);
      Assert.assertEquals(content, new byte[] { content[0], 2, 3, seed, 5, 6, (byte) fileNumber, (byte) (pageIndex & 0xFF) });
      return null;
    }
  }
}
