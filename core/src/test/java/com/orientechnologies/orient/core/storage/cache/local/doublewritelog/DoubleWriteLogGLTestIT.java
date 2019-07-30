package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.io.OFileUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

public class DoubleWriteLogGLTestIT {
  private static String buildDirectory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    buildDirectory += File.separator + DoubleWriteLogGLTestIT.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(buildDirectory));
    Files.createDirectories(Paths.get(buildDirectory));
  }

  @Test
  public void testWriteSinglePage() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

    doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
    try {
      OPointer pointer;

      final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
      ThreadLocalRandom random = ThreadLocalRandom.current();

      final byte[] data = new byte[pageSize];
      random.nextBytes(data);

      buffer.put(data);

      buffer.position(0);
      doubleWriteLog.write(new ByteBuffer[] { buffer }, 12, 24);
      doubleWriteLog.truncate();

      pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      Assert.assertNull(pointer);

      doubleWriteLog.restoreModeOn();

      pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      final ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

      Assert.assertEquals(256, loadedBuffer.limit());
      final byte[] loadedData = new byte[256];
      loadedBuffer.rewind();
      loadedBuffer.get(loadedData);

      Assert.assertArrayEquals(data, loadedData);
      bufferPool.release(pointer);
    } finally {
      doubleWriteLog.close();
    }

  }

  @Test
  public void testWriteSinglePageTwoTimes() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

    doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
    try {
      final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
      ThreadLocalRandom random = ThreadLocalRandom.current();

      final byte[] data = new byte[pageSize];
      random.nextBytes(data);

      buffer.put(data);

      doubleWriteLog.write(new ByteBuffer[] { buffer }, 12, 24);

      buffer.rewind();
      random.nextBytes(data);
      buffer.put(data);

      doubleWriteLog.write(new ByteBuffer[] { buffer }, 12, 24);
      doubleWriteLog.truncate();

      OPointer pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      Assert.assertNull(pointer);

      doubleWriteLog.restoreModeOn();

      pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      final ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

      Assert.assertEquals(256, loadedBuffer.limit());
      final byte[] loadedData = new byte[256];
      loadedBuffer.rewind();
      loadedBuffer.get(loadedData);

      Assert.assertArrayEquals(data, loadedData);
      bufferPool.release(pointer);
    } finally {
      doubleWriteLog.close();
    }
  }

  @Test
  public void testWriteTwoPagesSameFile() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

    doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
    try {
      ThreadLocalRandom random = ThreadLocalRandom.current();

      List<byte[]> datas = new ArrayList<>();
      for (int i = 0; i < 2; i++) {
        final byte[] data = new byte[pageSize];
        random.nextBytes(data);
        datas.add(data);
      }

      ByteBuffer[] buffers = new ByteBuffer[datas.size()];
      for (int i = 0; i < buffers.length; i++) {
        final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
        buffer.put(datas.get(i));
        buffers[i] = buffer;
      }

      doubleWriteLog.write(buffers, 12, 24);

      OPointer pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      Assert.assertNull(pointer);

      pointer = doubleWriteLog.loadPage(12, 25, bufferPool);
      Assert.assertNull(pointer);

      doubleWriteLog.restoreModeOn();

      pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

      Assert.assertEquals(256, loadedBuffer.limit());
      byte[] loadedData = new byte[256];
      loadedBuffer.rewind();
      loadedBuffer.get(loadedData);

      Assert.assertArrayEquals(datas.get(0), loadedData);
      bufferPool.release(pointer);

      pointer = doubleWriteLog.loadPage(12, 25, bufferPool);
      loadedBuffer = pointer.getNativeByteBuffer();

      Assert.assertEquals(256, loadedBuffer.limit());
      loadedData = new byte[256];
      loadedBuffer.rewind();
      loadedBuffer.get(loadedData);

      Assert.assertArrayEquals(datas.get(1), loadedData);
    } finally {
      doubleWriteLog.close();
    }
  }

  @Test
  public void testWriteTenPagesSameFile() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

    doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
    try {
      ThreadLocalRandom random = ThreadLocalRandom.current();

      List<byte[]> datas = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        final byte[] data = new byte[pageSize];
        random.nextBytes(data);
        datas.add(data);
      }

      ByteBuffer[] buffers = new ByteBuffer[datas.size()];
      for (int i = 0; i < buffers.length; i++) {
        final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
        buffer.put(datas.get(i));
        buffers[i] = buffer;
      }

      doubleWriteLog.write(buffers, 12, 24);

      OPointer pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      Assert.assertNull(pointer);

      pointer = doubleWriteLog.loadPage(12, 25, bufferPool);
      Assert.assertNull(pointer);

      doubleWriteLog.restoreModeOn();

      for (int i = 0; i < 10; i++) {
        pointer = doubleWriteLog.loadPage(12, 24 + i, bufferPool);
        ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

        Assert.assertEquals(256, loadedBuffer.limit());
        byte[] loadedData = new byte[256];
        loadedBuffer.rewind();
        loadedBuffer.get(loadedData);

        Assert.assertArrayEquals(datas.get(i), loadedData);
        bufferPool.release(pointer);
      }
    } finally {
      doubleWriteLog.close();
    }
  }

  @Test
  public void testWriteTenDifferentSinglePages() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

    doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
    try {
      ThreadLocalRandom random = ThreadLocalRandom.current();

      List<byte[]> datas = new ArrayList<>();
      for (int i = 0; i < 10; i++) {
        final byte[] data = new byte[pageSize];
        random.nextBytes(data);
        datas.add(data);
      }

      for (int i = 0; i < datas.size(); i++) {
        final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
        buffer.put(datas.get(i));

        doubleWriteLog.write(new ByteBuffer[] { buffer }, 12, 24 + i);
      }

      OPointer pointer = doubleWriteLog.loadPage(12, 24, bufferPool);
      Assert.assertNull(pointer);

      pointer = doubleWriteLog.loadPage(12, 25, bufferPool);
      Assert.assertNull(pointer);

      doubleWriteLog.restoreModeOn();

      for (int i = 0; i < 10; i++) {
        pointer = doubleWriteLog.loadPage(12, 24 + i, bufferPool);
        ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

        Assert.assertEquals(256, loadedBuffer.limit());
        byte[] loadedData = new byte[256];
        loadedBuffer.rewind();
        loadedBuffer.get(loadedData);

        Assert.assertArrayEquals(datas.get(i), loadedData);
        bufferPool.release(pointer);
      }
    } finally {
      doubleWriteLog.close();
    }
  }

  @Test
  public void testWriteTenDifferentPagesTenTimes() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

    doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
    try {
      ThreadLocalRandom random = ThreadLocalRandom.current();

      List<byte[]> datas = new ArrayList<>();
      for (int i = 0; i < 100; i++) {
        final byte[] data = new byte[pageSize];
        random.nextBytes(data);
        datas.add(data);
      }

      for (int i = 0; i < 10; i++) {
        ByteBuffer[] buffers = new ByteBuffer[10];

        for (int j = 0; j < 10; j++) {
          final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
          buffer.put(datas.get(i * 10 + j));
          buffers[j] = buffer;
        }

        doubleWriteLog.write(buffers, 12 + i, 24);
      }

      doubleWriteLog.restoreModeOn();

      for (int i = 0; i < 10; i++) {
        for (int j = 0; j < 10; j++) {
          final OPointer pointer = doubleWriteLog.loadPage(12 + i, 24 + j, bufferPool);

          ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

          Assert.assertEquals(256, loadedBuffer.limit());
          byte[] loadedData = new byte[256];
          loadedBuffer.rewind();
          loadedBuffer.get(loadedData);

          Assert.assertArrayEquals(datas.get(i * 10 + j), loadedData);
          bufferPool.release(pointer);
        }
      }
    } finally {
      doubleWriteLog.close();
    }
  }

  @Test
  public void testRandomWriteOne() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testRandomWriteOne : seed " + seed);

    Random random = new Random(seed);

    for (int n = 0; n < 10; n++) {
      System.out.println("Iteration - " + n);

      final int pageSize = 256;

      final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
      final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

      doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
      try {
        final int pagesToWrite = random.nextInt(20_000) + 100;
        int writtenPages = 0;

        List<byte[]> datas = new ArrayList<>();
        for (int i = 0; i < pagesToWrite; i++) {
          final byte[] data = new byte[pageSize];
          random.nextBytes(data);
          datas.add(data);
        }

        int pageIndex = 0;

        while (writtenPages < pagesToWrite) {
          final int pagesForSinglePatch = random.nextInt(pagesToWrite - writtenPages) + 1;
          ByteBuffer[] buffers = new ByteBuffer[pagesForSinglePatch];

          for (int j = 0; j < pagesForSinglePatch; j++) {
            final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
            buffer.put(datas.get(pageIndex + j));
            buffers[j] = buffer;
          }

          doubleWriteLog.write(buffers, 12, 24 + pageIndex);
          pageIndex += pagesForSinglePatch;
          writtenPages += pagesForSinglePatch;
        }

        doubleWriteLog.restoreModeOn();

        for (int i = 0; i < pagesToWrite; i++) {
          final OPointer pointer = doubleWriteLog.loadPage(12, 24 + i, bufferPool);

          ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

          Assert.assertEquals(256, loadedBuffer.limit());
          byte[] loadedData = new byte[256];
          loadedBuffer.rewind();
          loadedBuffer.get(loadedData);

          Assert.assertArrayEquals(datas.get(i), loadedData);
          bufferPool.release(pointer);
        }

      } finally {
        doubleWriteLog.close();
      }
    }
  }

}
