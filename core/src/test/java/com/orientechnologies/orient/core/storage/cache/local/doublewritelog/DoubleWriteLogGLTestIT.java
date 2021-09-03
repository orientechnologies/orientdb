package com.orientechnologies.orient.core.storage.cache.local.doublewritelog;

import static com.orientechnologies.orient.core.storage.cache.local.doublewritelog.DoubleWriteLogGL.DEFAULT_BLOCK_SIZE;

import com.orientechnologies.common.directmemory.OByteBufferPool;
import com.orientechnologies.common.directmemory.OPointer;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DoubleWriteLogGLTestIT {
  private static String buildDirectory;

  @BeforeClass
  public static void beforeClass() throws Exception {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty()) buildDirectory = ".";

    buildDirectory += File.separator + DoubleWriteLogGLTestIT.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(buildDirectory));
    Files.createDirectories(Paths.get(buildDirectory));
  }

  @Test
  public void testWriteSinglePage() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    try {
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
        doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24});
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

        pointer = doubleWriteLog.loadPage(12, 25, bufferPool);
        Assert.assertNull(pointer);
      } finally {
        doubleWriteLog.close();
      }
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testWriteSinglePageTwoTimes() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);

    try {
      final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

      doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
      try {
        final ByteBuffer buffer = ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
        ThreadLocalRandom random = ThreadLocalRandom.current();

        final byte[] data = new byte[pageSize];
        random.nextBytes(data);

        buffer.put(data);

        doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24});

        buffer.rewind();
        random.nextBytes(data);
        buffer.put(data);

        doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24});
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

        pointer = doubleWriteLog.loadPage(12, 25, bufferPool);
        Assert.assertNull(pointer);
      } finally {
        doubleWriteLog.close();
      }
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testWriteTwoPagesSameFile() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
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

        final ByteBuffer buffer =
            ByteBuffer.allocate(pageSize * datas.size()).order(ByteOrder.nativeOrder());
        for (final byte[] data : datas) {
          buffer.put(data);
        }

        doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24});

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
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testWriteTenPagesSameFile() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
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

        ByteBuffer buffer =
            ByteBuffer.allocate(datas.size() * pageSize).order(ByteOrder.nativeOrder());
        for (final byte[] data : datas) {
          buffer.put(data);
        }

        doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24});

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
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testWriteTenDifferentSinglePages() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
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

        final ByteBuffer buffer =
            ByteBuffer.allocate(pageSize * datas.size()).order(ByteOrder.nativeOrder());
        for (byte[] data : datas) {
          buffer.put(data);
        }
        doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24});

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
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testWriteTenDifferentPagesTenTimes() throws Exception {
    final int pageSize = 256;

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
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
          ByteBuffer buffer = ByteBuffer.allocate(10 * pageSize).order(ByteOrder.nativeOrder());

          for (int j = 0; j < 10; j++) {
            buffer.put(datas.get(i * 10 + j));
          }

          doubleWriteLog.write(new ByteBuffer[] {buffer}, new int[] {12 + i}, new int[] {24});
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
    } finally {
      bufferPool.clear();
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
      try {
        final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

        doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
        try {
          final int pagesToWrite = random.nextInt(20_000) + 100;

          List<byte[]> datas = new ArrayList<>();
          for (int i = 0; i < pagesToWrite; i++) {
            final byte[] data = new byte[pageSize];
            random.nextBytes(data);
            datas.add(data);
          }

          int pageIndex = 0;
          int writtenPages = 0;

          while (writtenPages < pagesToWrite) {
            final int pagesForSinglePatch = random.nextInt(pagesToWrite - writtenPages) + 1;
            final ByteBuffer buffer =
                ByteBuffer.allocate(pagesForSinglePatch * pageSize).order(ByteOrder.nativeOrder());

            for (int j = 0; j < pagesForSinglePatch; j++) {
              buffer.put(datas.get(pageIndex + j));
            }

            doubleWriteLog.write(
                new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24 + pageIndex});
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
      } finally {
        bufferPool.clear();
      }
    }
  }

  @Test
  public void testRandomWriteTwo() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testRandomWriteTwo : seed " + seed);

    final Random random = new Random(seed);
    final int pageSize = 256;

    for (int n = 0; n < 10; n++) {
      System.out.println("Iteration - " + n);

      final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
      try {
        final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);
        doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
        try {
          final Map<Integer, ByteBuffer> pageMap = new HashMap<>();
          final int pages = random.nextInt(900) + 100;

          System.out.println("testRandomWriteTwo : pages " + pages);

          for (int k = 0; k < 100; k++) {
            final int pagesToWrite = random.nextInt(pages - 1) + 1;

            List<byte[]> datas = new ArrayList<>();
            for (int i = 0; i < pagesToWrite; i++) {
              final byte[] data = new byte[pageSize];
              random.nextBytes(data);
              datas.add(data);
            }

            final int startPageIndex = random.nextInt(pages);
            int pageIndex = 0;
            int writtenPages = 0;

            while (writtenPages < pagesToWrite) {
              final int pagesForSinglePatch = random.nextInt(pagesToWrite - writtenPages) + 1;
              ByteBuffer[] buffers = new ByteBuffer[pagesForSinglePatch];

              ByteBuffer containerBuffer =
                  ByteBuffer.allocate(pagesForSinglePatch * pageSize)
                      .order(ByteOrder.nativeOrder());
              for (int j = 0; j < pagesForSinglePatch; j++) {
                final ByteBuffer buffer =
                    ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
                buffer.put(datas.get(pageIndex + j));
                buffers[j] = buffer;

                buffer.rewind();
                containerBuffer.put(buffer);
                buffer.rewind();
              }

              doubleWriteLog.write(
                  new ByteBuffer[] {containerBuffer},
                  new int[] {12},
                  new int[] {startPageIndex + pageIndex});

              for (int j = 0; j < buffers.length; j++) {
                pageMap.put(startPageIndex + pageIndex + j, buffers[j]);
              }

              pageIndex += pagesForSinglePatch;
              writtenPages += pagesForSinglePatch;
            }
          }

          doubleWriteLog.restoreModeOn();

          for (final int pageIndex : pageMap.keySet()) {
            final OPointer pointer = doubleWriteLog.loadPage(12, pageIndex, bufferPool);

            final ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

            Assert.assertEquals(pageSize, loadedBuffer.limit());
            final byte[] loadedData = new byte[pageSize];

            loadedBuffer.rewind();
            loadedBuffer.get(loadedData);

            final byte[] data = new byte[pageSize];
            final ByteBuffer buffer = pageMap.get(pageIndex);

            buffer.rewind();
            buffer.get(data);

            Assert.assertArrayEquals(data, loadedData);
            bufferPool.release(pointer);
          }

        } finally {
          doubleWriteLog.close();
        }
      } finally {
        bufferPool.clear();
      }
    }
  }

  @Test
  public void testRandomCrashOne() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testRandomCrashOne : seed " + seed);

    Random random = new Random(seed);

    for (int n = 0; n < 10; n++) {
      System.out.println("Iteration - " + n);

      final int pageSize = 256;

      final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
      try {
        final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);

        doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
        try {
          final int pagesToWrite = random.nextInt(20_000) + 100;

          List<byte[]> datas = new ArrayList<>();
          for (int i = 0; i < pagesToWrite; i++) {
            final byte[] data = new byte[pageSize];
            random.nextBytes(data);
            datas.add(data);
          }

          int pageIndex = 0;
          int writtenPages = 0;

          while (writtenPages < pagesToWrite) {
            final int pagesForSinglePatch = random.nextInt(pagesToWrite - writtenPages) + 1;
            ByteBuffer buffer =
                ByteBuffer.allocate(pagesForSinglePatch * pageSize).order(ByteOrder.nativeOrder());

            for (int j = 0; j < pagesForSinglePatch; j++) {
              buffer.put(datas.get(pageIndex + j));
            }

            doubleWriteLog.write(
                new ByteBuffer[] {buffer}, new int[] {12}, new int[] {24 + pageIndex});
            pageIndex += pagesForSinglePatch;
            writtenPages += pagesForSinglePatch;
          }

          final DoubleWriteLogGL doubleWriteLogRestore = new DoubleWriteLogGL(2 * 4 * 1024);
          doubleWriteLogRestore.open("test", Paths.get(buildDirectory), pageSize);

          doubleWriteLogRestore.restoreModeOn();

          for (int i = 0; i < pagesToWrite; i++) {
            final OPointer pointer = doubleWriteLogRestore.loadPage(12, 24 + i, bufferPool);

            ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

            Assert.assertEquals(256, loadedBuffer.limit());
            byte[] loadedData = new byte[256];
            loadedBuffer.rewind();
            loadedBuffer.get(loadedData);

            Assert.assertArrayEquals(datas.get(i), loadedData);
            bufferPool.release(pointer);
          }

          doubleWriteLogRestore.close();
        } finally {
          doubleWriteLog.close();
        }
      } finally {
        bufferPool.clear();
      }
    }
  }

  @Test
  public void testRandomWriteCrashTwo() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testRandomCrashTwo : seed " + seed);

    final Random random = new Random(seed);
    final int pageSize = 256;

    for (int n = 0; n < 10; n++) {
      System.out.println("Iteration - " + n);

      final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
      try {
        final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(2 * 4 * 1024);
        doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
        try {
          final Map<Integer, ByteBuffer> pageMap = new HashMap<>();
          final int pages = random.nextInt(900) + 100;

          System.out.println("testRandomCrashTwo : pages " + pages);

          for (int k = 0; k < 100; k++) {
            final int pagesToWrite = random.nextInt(pages - 1) + 1;

            List<byte[]> datas = new ArrayList<>();
            for (int i = 0; i < pagesToWrite; i++) {
              final byte[] data = new byte[pageSize];
              random.nextBytes(data);
              datas.add(data);
            }

            final int startPageIndex = random.nextInt(pages);
            int pageIndex = 0;
            int writtenPages = 0;

            while (writtenPages < pagesToWrite) {
              final int pagesForSinglePatch = random.nextInt(pagesToWrite - writtenPages) + 1;
              ByteBuffer[] buffers = new ByteBuffer[pagesForSinglePatch];
              ByteBuffer containerBuffer =
                  ByteBuffer.allocate(pagesForSinglePatch * pageSize)
                      .order(ByteOrder.nativeOrder());

              for (int j = 0; j < pagesForSinglePatch; j++) {
                final ByteBuffer buffer =
                    ByteBuffer.allocate(pageSize).order(ByteOrder.nativeOrder());
                buffer.put(datas.get(pageIndex + j));
                buffer.rewind();

                buffers[j] = buffer;
                containerBuffer.put(buffer);
              }

              doubleWriteLog.write(
                  new ByteBuffer[] {containerBuffer},
                  new int[] {12},
                  new int[] {startPageIndex + pageIndex});
              for (int j = 0; j < buffers.length; j++) {
                pageMap.put(startPageIndex + pageIndex + j, buffers[j]);
              }

              pageIndex += pagesForSinglePatch;
              writtenPages += pagesForSinglePatch;
            }
          }

          final DoubleWriteLogGL doubleWriteLogRestore = new DoubleWriteLogGL(2 * 4 * 1024);
          doubleWriteLogRestore.open("test", Paths.get(buildDirectory), pageSize);

          doubleWriteLogRestore.restoreModeOn();

          for (final int pageIndex : pageMap.keySet()) {
            final OPointer pointer = doubleWriteLogRestore.loadPage(12, pageIndex, bufferPool);

            final ByteBuffer loadedBuffer = pointer.getNativeByteBuffer();

            Assert.assertEquals(pageSize, loadedBuffer.limit());
            final byte[] loadedData = new byte[pageSize];

            loadedBuffer.rewind();
            loadedBuffer.get(loadedData);

            final byte[] data = new byte[pageSize];
            final ByteBuffer buffer = pageMap.get(pageIndex);

            buffer.rewind();
            buffer.get(data);

            Assert.assertArrayEquals(data, loadedData);
            bufferPool.release(pointer);
          }

          doubleWriteLogRestore.close();
        } finally {
          doubleWriteLog.close();
        }
      } finally {
        bufferPool.clear();
      }
    }
  }

  @Test
  public void testTruncate() throws IOException {
    final int pageSize = 256;
    final int maxLogSize = blockSize(); // single block for each segment

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
      final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(maxLogSize);
      doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
      try {
        List<Path> paths =
            Arrays.asList(Files.list(Paths.get(buildDirectory)).toArray(Path[]::new));
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());

        for (int i = 0; i < 4; i++) {
          final boolean overflow =
              doubleWriteLog.write(
                  new ByteBuffer[] {ByteBuffer.allocate(pageSize)}, new int[] {12}, new int[] {45});
          Assert.assertTrue(overflow && i > 0 || i == 0 && !overflow);
        }

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));
        Assert.assertEquals(4, paths.size());

        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(1).getFileName().toString());
        Assert.assertEquals(
            "test_2" + DoubleWriteLogGL.EXTENSION, paths.get(2).getFileName().toString());
        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(3).getFileName().toString());

        doubleWriteLog.restoreModeOn();
        doubleWriteLog.truncate();

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));
        Assert.assertEquals(4, paths.size());

        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(1).getFileName().toString());
        Assert.assertEquals(
            "test_2" + DoubleWriteLogGL.EXTENSION, paths.get(2).getFileName().toString());
        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(3).getFileName().toString());

        doubleWriteLog.restoreModeOff();

        doubleWriteLog.truncate();

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));
        Assert.assertEquals(1, paths.size());

        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
      } finally {
        doubleWriteLog.close();
      }
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testClose() throws IOException {
    final int pageSize = 256;
    final int maxLogSize = blockSize(); // single block for each segment

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
      final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(maxLogSize);
      doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
      try {
        List<Path> paths =
            Arrays.asList(Files.list(Paths.get(buildDirectory)).toArray(Path[]::new));
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());

        for (int i = 0; i < 4; i++) {
          doubleWriteLog.write(
              new ByteBuffer[] {ByteBuffer.allocate(pageSize)}, new int[] {12}, new int[] {45});
        }

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));
        Assert.assertEquals(4, paths.size());

        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(1).getFileName().toString());
        Assert.assertEquals(
            "test_2" + DoubleWriteLogGL.EXTENSION, paths.get(2).getFileName().toString());
        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(3).getFileName().toString());

      } finally {
        doubleWriteLog.close();
      }
    } finally {
      bufferPool.clear();
    }

    final List<Path> paths =
        Arrays.asList(Files.list(Paths.get(buildDirectory)).toArray(Path[]::new));
    Assert.assertTrue(paths.isEmpty());
  }

  @Test
  public void testInitAfterCrash() throws Exception {
    final int pageSize = 256;
    final int maxLogSize = blockSize(); // single block for each segment

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
      final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(maxLogSize);
      doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
      try {
        for (int i = 0; i < 4; i++) {
          doubleWriteLog.write(
              new ByteBuffer[] {ByteBuffer.allocate(pageSize)}, new int[] {12}, new int[] {45});
        }

        List<Path> paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));
        Assert.assertEquals(4, paths.size());

        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(1).getFileName().toString());
        Assert.assertEquals(
            "test_2" + DoubleWriteLogGL.EXTENSION, paths.get(2).getFileName().toString());
        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(3).getFileName().toString());

        final DoubleWriteLogGL doubleWriteLogRestore = new DoubleWriteLogGL(maxLogSize);
        doubleWriteLogRestore.open("test", Paths.get(buildDirectory), pageSize);

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));
        Assert.assertEquals(5, paths.size());

        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(1).getFileName().toString());
        Assert.assertEquals(
            "test_2" + DoubleWriteLogGL.EXTENSION, paths.get(2).getFileName().toString());
        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(3).getFileName().toString());
        Assert.assertEquals(
            "test_4" + DoubleWriteLogGL.EXTENSION, paths.get(4).getFileName().toString());

        doubleWriteLogRestore.close();
      } finally {
        doubleWriteLog.close();
      }
    } finally {
      bufferPool.clear();
    }
  }

  @Test
  public void testCreationNewSegment() throws Exception {
    final int pageSize = 256;
    final int maxLogSize = blockSize(); // single block for each segment

    final OByteBufferPool bufferPool = new OByteBufferPool(pageSize);
    try {
      final DoubleWriteLogGL doubleWriteLog = new DoubleWriteLogGL(maxLogSize);
      doubleWriteLog.open("test", Paths.get(buildDirectory), pageSize);
      try {
        List<Path> paths =
            Arrays.asList(Files.list(Paths.get(buildDirectory)).toArray(Path[]::new));
        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(
            "test_0" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());

        doubleWriteLog.startCheckpoint();
        doubleWriteLog.truncate();

        for (int i = 0; i < 4; i++) {
          final boolean overflow =
              doubleWriteLog.write(
                  new ByteBuffer[] {ByteBuffer.allocate(pageSize)}, new int[] {12}, new int[] {45});
          Assert.assertFalse(overflow);
        }

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));

        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());

        doubleWriteLog.endCheckpoint();

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));

        Assert.assertEquals(1, paths.size());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());

        for (int i = 0; i < 4; i++) {
          final boolean overflow =
              doubleWriteLog.write(
                  new ByteBuffer[] {ByteBuffer.allocate(pageSize)}, new int[] {12}, new int[] {45});
          Assert.assertTrue(overflow);
        }

        paths =
            Arrays.asList(
                Files.list(Paths.get(buildDirectory))
                    .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                    .toArray(Path[]::new));

        Assert.assertEquals(5, paths.size());
        Assert.assertEquals(
            "test_1" + DoubleWriteLogGL.EXTENSION, paths.get(0).getFileName().toString());
        Assert.assertEquals(
            "test_2" + DoubleWriteLogGL.EXTENSION, paths.get(1).getFileName().toString());
        Assert.assertEquals(
            "test_3" + DoubleWriteLogGL.EXTENSION, paths.get(2).getFileName().toString());
        Assert.assertEquals(
            "test_4" + DoubleWriteLogGL.EXTENSION, paths.get(3).getFileName().toString());
        Assert.assertEquals(
            "test_5" + DoubleWriteLogGL.EXTENSION, paths.get(4).getFileName().toString());
      } finally {
        doubleWriteLog.close();
      }
    } finally {
      bufferPool.clear();
    }
  }

  private int blockSize() {
    int blockSize =
        OIOUtils.calculateBlockSize(Paths.get(buildDirectory).toAbsolutePath().toString());
    if (blockSize == -1) {
      blockSize = DEFAULT_BLOCK_SIZE;
    }

    return blockSize;
  }
}
