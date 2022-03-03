package com.orientechnologies.common.io;

import com.orientechnologies.common.util.ORawPair;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.text.ParseException;
import java.util.*;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

public class OIOUtilsTest {

  @Test
  public void shouldGetTimeAsMilis() {
    assertGetTimeAsMilis("2h", 2 * 3600 * 1000);
    assertGetTimeAsMilis("500ms", 500);
    assertGetTimeAsMilis("4d", 4 * 24 * 3600 * 1000);
    assertGetTimeAsMilis("6w", 6l * 7 * 24 * 3600 * 1000);
  }

  private void assertGetTimeAsMilis(String data, long expected) {
    assertEquals(OIOUtils.getTimeAsMillisecs(data), expected);
  }

  @Test
  public void shoudGetRightTimeFromString() throws ParseException {
    Calendar calendar = Calendar.getInstance();
    calendar.set(Calendar.HOUR_OF_DAY, 5);
    calendar.set(Calendar.MINUTE, 10);
    calendar.set(Calendar.SECOND, 0);
    calendar.set(Calendar.MILLISECOND, 0);
    Date d = OIOUtils.getTodayWithTime("05:10:00");
    assertEquals(calendar.getTime(), d);
  }

  @Test
  public void shouldReadFileAsString() throws IOException {
    // UTF-8
    Path path = Paths.get("./src/test/resources/", getClass().getSimpleName() + "_utf8.txt");

    String asString = OIOUtils.readFileAsString(path.toFile());

    assertThat(asString).isEqualToIgnoringCase("utf-8 :: èàòì€");

    // ISO-8859-1
    path = Paths.get("./src/test/resources/", getClass().getSimpleName() + "_iso-8859-1.txt");

    asString = OIOUtils.readFileAsString(path.toFile());

    assertThat(asString).isNotEqualToIgnoringCase("iso-8859-1 :: èàòì?");
  }

  @Test
  public void shouldReadFileAsStringWithGivenCharset() throws IOException {
    // UTF-8
    Path path = Paths.get("./src/test/resources/", getClass().getSimpleName() + "_utf8.txt");

    String asString = OIOUtils.readFileAsString(path.toFile(), StandardCharsets.UTF_8);

    assertThat(asString).isEqualToIgnoringCase("utf-8 :: èàòì€");

    // ISO-8859-1
    path = Paths.get("./src/test/resources/", getClass().getSimpleName() + "_iso-8859-1.txt");

    asString = OIOUtils.readFileAsString(path.toFile(), StandardCharsets.ISO_8859_1);

    assertThat(asString).isEqualToIgnoringCase("iso-8859-1 :: èàòì?");
  }

  @Test
  public void testWriteByteBuffersSingleBuffer() throws IOException {
    final Path path =
        Paths.get("./target/", getClass().getSimpleName() + "testWriteByteBuffersSingleBuffer.tst");
    Files.deleteIfExists(path);

    try (final AsynchronousFileChannel fileChannel =
        AsynchronousFileChannel.open(
            path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {

      final ByteBuffer writeByteBuffer = ByteBuffer.allocate(4);
      writeByteBuffer.putInt(42);
      writeByteBuffer.rewind();

      final ArrayList<ORawPair<Long, ByteBuffer>> data = new ArrayList<>();
      data.add(new ORawPair<>(12L, writeByteBuffer));

      OIOUtils.writeByteBuffers(data, fileChannel, 1024);

      final ByteBuffer readByteBuffer = ByteBuffer.allocate(4);

      OIOUtils.readByteBuffer(readByteBuffer, fileChannel, 12, true);
      readByteBuffer.rewind();

      Assert.assertEquals(42, readByteBuffer.getInt());
    } finally {
      Files.deleteIfExists(path);
    }
  }

  @Test
  public void testWriteByteBuffersSeveralBuffersRandomBellowMaxLimit() throws Exception {
    final Path path =
        Paths.get(
            "./target/",
            getClass().getSimpleName()
                + "testWriteByteBuffersSeveralBuffersRandomBellowMaxLimit.tst");
    Files.deleteIfExists(path);

    try (final AsynchronousFileChannel fileChannel =
        AsynchronousFileChannel.open(
            path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {
      final long seed = System.nanoTime();

      System.out.println("OIOUtilsTest#testWriteByteBuffersSeveralBuffersRandom seed : " + seed);
      final Random random = new Random(seed);

      final int chunkSize = 256;
      final int maxPages = 10 * 1024;

      final int chunksCount = random.nextInt(1000) + 4;
      final ArrayList<ORawPair<Long, ByteBuffer>> chunks = new ArrayList<>();

      final HashSet<Integer> pages = new HashSet<>();

      for (int i = 0; i < chunksCount; i++) {
        final ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        random.nextBytes(buffer.array());

        int pageIndex = random.nextInt(maxPages);

        while (!pages.add(pageIndex)) {
          pageIndex = random.nextInt(maxPages);
        }

        chunks.add(new ORawPair<>((long)pageIndex * chunkSize, buffer));
      }

      OIOUtils.writeByteBuffers(chunks, fileChannel, 1024);

      for (final ORawPair<Long, ByteBuffer> chunk : chunks) {
        final ByteBuffer buffer = ByteBuffer.allocate(chunk.second.capacity());
        OIOUtils.readByteBuffer(buffer, fileChannel, chunk.first, true);
        Assert.assertArrayEquals(chunk.second.array(), buffer.array());
      }
    } finally {
      Files.deleteIfExists(path);
    }
  }

  @Test
  public void testWriteByteBuffersSeveralBuffersRandomEqualToMaxLimit() throws Exception {
    final Path path =
        Paths.get(
            "./target/",
            getClass().getSimpleName()
                + "testWriteByteBuffersSeveralBuffersRandomEqualToMaxLimit.tst");
    Files.deleteIfExists(path);

    try (final AsynchronousFileChannel fileChannel =
        AsynchronousFileChannel.open(
            path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {
      final long seed = System.nanoTime();

      System.out.println(
          "OIOUtilsTest#testWriteByteBuffersSeveralBuffersRandomEqualToMaxLimit seed : " + seed);
      final Random random = new Random(seed);

      final int chunkSize = 256;
      final int maxPages = 10 * 1024;

      final int chunksCount = random.nextInt(1000) + 4;
      final ArrayList<ORawPair<Long, ByteBuffer>> chunks = new ArrayList<>();

      final HashSet<Integer> pages = new HashSet<>();

      for (int i = 0; i < chunksCount; i++) {
        final ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        random.nextBytes(buffer.array());

        int pageIndex = random.nextInt(maxPages);

        while (!pages.add(pageIndex)) {
          pageIndex = random.nextInt(maxPages);
        }

        chunks.add(new ORawPair<>((long)pageIndex * chunkSize, buffer));
      }

      OIOUtils.writeByteBuffers(chunks, fileChannel, chunksCount);

      for (final ORawPair<Long, ByteBuffer> chunk : chunks) {
        final ByteBuffer buffer = ByteBuffer.allocate(chunk.second.capacity());
        OIOUtils.readByteBuffer(buffer, fileChannel, chunk.first, true);
        Assert.assertArrayEquals(chunk.second.array(), buffer.array());
      }

    } finally {
      Files.deleteIfExists(path);
    }
  }

  @Test
  public void testWriteByteBuffersSeveralBuffersRandomLessToMaxLimit() throws Exception {
    final Path path =
        Paths.get(
            "./target/",
            getClass().getSimpleName()
                + "testWriteByteBuffersSeveralBuffersRandomLessToMaxLimit.tst");
    Files.deleteIfExists(path);

    try (final AsynchronousFileChannel fileChannel =
        AsynchronousFileChannel.open(
            path, StandardOpenOption.WRITE, StandardOpenOption.READ, StandardOpenOption.CREATE)) {
      final long seed = System.nanoTime();

      System.out.println(
          "OIOUtilsTest#testWriteByteBuffersSeveralBuffersRandomLessToMaxLimit seed : " + seed);
      final Random random = new Random(seed);

      final int chunkSize = 256;
      final int maxPages = 10 * 1024;

      final int chunksCount = random.nextInt(1000) + 4;
      final ArrayList<ORawPair<Long, ByteBuffer>> chunks = new ArrayList<>();

      final HashSet<Integer> pages = new HashSet<>();

      for (int i = 0; i < chunksCount; i++) {
        final ByteBuffer buffer = ByteBuffer.allocate(chunkSize);
        random.nextBytes(buffer.array());

        int pageIndex = random.nextInt(maxPages);

        while (!pages.add(pageIndex)) {
          pageIndex = random.nextInt(maxPages);
        }

        chunks.add(new ORawPair<>((long)pageIndex * chunkSize, buffer));
      }

      OIOUtils.writeByteBuffers(chunks, fileChannel, chunksCount - 2);

      for (final ORawPair<Long, ByteBuffer> chunk : chunks) {
        final ByteBuffer buffer = ByteBuffer.allocate(chunk.second.capacity());
        OIOUtils.readByteBuffer(buffer, fileChannel, chunk.first, true);
        Assert.assertArrayEquals(chunk.second.array(), buffer.array());
      }
    } finally {
      Files.deleteIfExists(path);
    }
  }
}
