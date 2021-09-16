package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.mmap;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.common.WriteableWALRecord;

import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class ReadOnlyWALSegmentTest {
  private static Path testDirectory;

  @BeforeClass
  public static void beforeClass() {
    testDirectory =
        Paths.get(
            System.getProperty(
                "buildDirectory" + File.separator + ReadOnlyWALSegmentTest.class.getName(),
                "."
                    + File.separator
                    + "target"
                    + File.separator
                    + ReadOnlyWALSegmentTest.class.getName()));

    OWALRecordsFactory.INSTANCE.registerNewRecord(1024, TestRecord.class);
  }

  @Before
  public void before() throws Exception {
    OFileUtils.deleteRecursively(testDirectory.toFile());
    Files.createDirectories(testDirectory);
  }

  @Test
  public void testSingleRecordAddition() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testSingleRecordAddition seed : " + seed);
    final Random random = new Random(seed);
    final TestRecord testRecord = new TestRecord(random, 128, 64);
    OLogSequenceNumber recLsn;

    try (final ModifiableWALSegment modifiableWALSegment =
        new ModifiableWALSegment(testDirectory.resolve("seg.wal"), 1024, 42)) {
      Optional<OLogSequenceNumber> oRecLSN = modifiableWALSegment.write(testRecord);
      Assert.assertTrue(oRecLSN.isPresent());

      recLsn = oRecLSN.get();
    }

    try (final ReadOnlyWALSegment readOnlyWALSegment =
        new ReadOnlyWALSegment(testDirectory.resolve("seg.wal"), 42)) {

      final Optional<OLogSequenceNumber> begin = readOnlyWALSegment.begin();
      final Optional<OLogSequenceNumber> end = readOnlyWALSegment.end();

      Assert.assertTrue(begin.isPresent());
      Assert.assertTrue(end.isPresent());

      Assert.assertEquals(recLsn, begin.get());
      Assert.assertEquals(recLsn, end.get());

      final Optional<WriteableWALRecord> loadedRecord = readOnlyWALSegment.read(recLsn);
      Assert.assertTrue(loadedRecord.isPresent());

      final TestRecord loadedTestRecord = (TestRecord) loadedRecord.get();
      Assert.assertEquals(recLsn, loadedTestRecord.getLsn());

      Assert.assertArrayEquals(testRecord.data, loadedTestRecord.data);
    }
  }

  @Test
  public void testAdditionTwoRecords() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testAdditionTwoRecords seed : " + seed);
    final Random random = new Random(seed);
    final OLogSequenceNumber lsnOne;
    final OLogSequenceNumber lsnTwo;

    final TestRecord testRecordOne;
    final TestRecord testRecordTwo;

    try (final ModifiableWALSegment modifiableWALSegment =
        new ModifiableWALSegment(testDirectory.resolve("seg.wal"), 1024, 42)) {

      testRecordOne = new TestRecord(random, 128, 64);
      testRecordTwo = new TestRecord(random, 128, 64);

      final Optional<OLogSequenceNumber> oLsnOne = modifiableWALSegment.write(testRecordOne);
      Assert.assertTrue(oLsnOne.isPresent());
      lsnOne = oLsnOne.get();

      Assert.assertEquals(lsnOne, testRecordOne.getLsn());

      final Optional<OLogSequenceNumber> oLsnTwo = modifiableWALSegment.write(testRecordTwo);
      Assert.assertTrue(oLsnTwo.isPresent());

      lsnTwo = oLsnTwo.get();
    }

    try (final ReadOnlyWALSegment readOnlyWALSegment =
        new ReadOnlyWALSegment(testDirectory.resolve("seg.wal"), 42)) {
      final Optional<OLogSequenceNumber> begin = readOnlyWALSegment.begin();
      final Optional<OLogSequenceNumber> end = readOnlyWALSegment.end();

      Assert.assertTrue(begin.isPresent());
      Assert.assertTrue(end.isPresent());

      Assert.assertEquals(lsnOne, begin.get());
      Assert.assertEquals(lsnTwo, end.get());

      final Optional<WriteableWALRecord> loadedRecordOne = readOnlyWALSegment.read(lsnOne);
      Assert.assertTrue(loadedRecordOne.isPresent());

      final Optional<WriteableWALRecord> loadedRecordTwo = readOnlyWALSegment.read(lsnTwo);
      Assert.assertTrue(loadedRecordTwo.isPresent());

      final TestRecord loadedTestRecordOne = (TestRecord) loadedRecordOne.get();
      final TestRecord loadedTestRecordTwo = (TestRecord) loadedRecordTwo.get();

      Assert.assertEquals(lsnOne, testRecordOne.getLsn());
      Assert.assertEquals(lsnTwo, testRecordTwo.getLsn());

      Assert.assertArrayEquals(testRecordOne.data, loadedTestRecordOne.data);
      Assert.assertArrayEquals(testRecordTwo.data, loadedTestRecordTwo.data);

      Optional<WriteableWALRecord> nextLoadedRecord = readOnlyWALSegment.next(lsnOne);
      Assert.assertTrue(nextLoadedRecord.isPresent());

      TestRecord nextTestRecord = (TestRecord) nextLoadedRecord.get();
      Assert.assertEquals(lsnTwo, nextTestRecord.getLsn());

      Assert.assertArrayEquals(testRecordTwo.data, nextTestRecord.data);

      nextLoadedRecord = readOnlyWALSegment.next(testRecordOne);
      Assert.assertTrue(nextLoadedRecord.isPresent());

      nextTestRecord = (TestRecord) nextLoadedRecord.get();
      Assert.assertEquals(lsnTwo, nextTestRecord.getLsn());

      Assert.assertArrayEquals(testRecordTwo.data, nextTestRecord.data);
    }
  }

  @Test
  public void testAdditionTillFull() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testAdditionTillFull seed : " + seed);
    final Random random = new Random(seed);

    final List<TestRecord> writtenRecords = new ArrayList<>();
    try (final ModifiableWALSegment modifiableWALSegment =
        new ModifiableWALSegment(testDirectory.resolve("seg.wal"), 1024, 42)) {
      final TestRecord testRecord = new TestRecord(random, 128, 64);
      modifiableWALSegment.write(testRecord);
      writtenRecords.add(testRecord);
    }

    try (final ReadOnlyWALSegment readOnlyWALSegment =
        new ReadOnlyWALSegment(testDirectory.resolve("seg.wal"), 42)) {

      final Optional<OLogSequenceNumber> begin = readOnlyWALSegment.begin();
      final Optional<OLogSequenceNumber> end = readOnlyWALSegment.end();

      Assert.assertTrue(begin.isPresent());
      Assert.assertTrue(end.isPresent());

      for (final TestRecord testRecord : writtenRecords) {
        final OLogSequenceNumber recLsn = testRecord.getLsn();
        final Optional<WriteableWALRecord> loadedRecord = readOnlyWALSegment.read(recLsn);
        Assert.assertTrue(loadedRecord.isPresent());

        final TestRecord loadedTestRecord = (TestRecord) loadedRecord.get();
        Assert.assertEquals(recLsn, loadedTestRecord.getLsn());
        Assert.assertArrayEquals(testRecord.data, loadedTestRecord.data);
      }

      for (int i = 0; i < writtenRecords.size(); i++) {
        TestRecord tRecordOrigin = writtenRecords.get(i);

        for (TestRecord tRecord : writtenRecords.subList(i + 1, writtenRecords.size())) {
          final Optional<WriteableWALRecord> rRecord = readOnlyWALSegment.next(tRecordOrigin);

          Assert.assertTrue(rRecord.isPresent());
          final TestRecord lRecord = (TestRecord) rRecord.get();
          Assert.assertArrayEquals(tRecord.data, lRecord.data);
          Assert.assertEquals(tRecord.getLsn(), lRecord.getLsn());

          tRecordOrigin = tRecord;
        }

        final Optional<WriteableWALRecord> rRecord = readOnlyWALSegment.next(tRecordOrigin);
        Assert.assertFalse(rRecord.isPresent());
      }

      for (int i = 0; i < writtenRecords.size(); i++) {
        TestRecord tRecordOrigin = writtenRecords.get(i);

        for (TestRecord tRecord : writtenRecords.subList(i + 1, writtenRecords.size())) {
          final Optional<WriteableWALRecord> rRecord =
              readOnlyWALSegment.next(tRecordOrigin.getLsn());

          Assert.assertTrue(rRecord.isPresent());
          final TestRecord lRecord = (TestRecord) rRecord.get();
          Assert.assertArrayEquals(tRecord.data, lRecord.data);
          Assert.assertEquals(tRecord.getLsn(), lRecord.getLsn());

          tRecordOrigin = tRecord;
        }

        final Optional<WriteableWALRecord> rRecord = readOnlyWALSegment.next(tRecordOrigin);
        Assert.assertFalse(rRecord.isPresent());
      }
    }
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[] data;

    @SuppressWarnings("unused")
    public TestRecord() {}

    @Override
    public boolean trackOperationId() {
      return true;
    }

    @SuppressWarnings("unused")
    public TestRecord(byte[] data) {
      this.data = data;
    }

    TestRecord(Random random, int maxSize, int minSize) {
      int len = random.nextInt(maxSize - minSize + 1) + 1;
      data = new byte[len];
      random.nextBytes(data);
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
    public void fromStream(ByteBuffer buffer) {
      final int dataLen = buffer.getInt();
      final byte[] data = new byte[dataLen];
      buffer.get(data);
      this.data = data;
    }

    @Override
    public int serializedSize() {
      return data.length + OIntegerSerializer.INT_SIZE;
    }

    @Override
    public int getId() {
      return 1024;
    }
  }
}
