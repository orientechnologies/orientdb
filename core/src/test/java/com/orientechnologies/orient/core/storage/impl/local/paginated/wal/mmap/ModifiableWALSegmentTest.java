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

public class ModifiableWALSegmentTest {
  private static Path testDirectory;

  @BeforeClass
  public static void beforeClass() {
    testDirectory =
        Paths.get(
            System.getProperty(
                "buildDirectory" + File.separator + ModifiableWALSegmentTest.class.getName(),
                "."
                    + File.separator
                    + "target"
                    + File.separator
                    + ModifiableWALSegmentTest.class.getName()));

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
    try (final ModifiableWALSegment modifiableWALSegment =
        new ModifiableWALSegment(testDirectory.resolve("seg.wal"), 1024, 42)) {
      final TestRecord testRecord = new TestRecord(random, 128, 64);

      @SuppressWarnings("OptionalGetWithoutIsPresent")
      final OLogSequenceNumber lsn = modifiableWALSegment.write(testRecord).get();

      Assert.assertEquals(lsn, testRecord.getLsn());

      verifySingleRecordAddition(modifiableWALSegment, testRecord, lsn);

      modifiableWALSegment.sync();

      verifySingleRecordAddition(modifiableWALSegment, testRecord, lsn);

      modifiableWALSegment.sync();

      verifySingleRecordAddition(modifiableWALSegment, testRecord, lsn);
    }
  }

  private void verifySingleRecordAddition(
      ModifiableWALSegment modifiableWALSegment, TestRecord testRecord, OLogSequenceNumber lsn) {
    Optional<WriteableWALRecord> readRecord = modifiableWALSegment.read(lsn);
    Assert.assertTrue(readRecord.isPresent());

    WriteableWALRecord record = readRecord.get();
    Assert.assertArrayEquals(testRecord.data, ((TestRecord) record).data);
    Assert.assertEquals(lsn, record.getLsn());
  }

  @Test
  public void testAdditionTwoRecords() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testAdditionTwoRecords seed : " + seed);
    final Random random = new Random(seed);
    try (final ModifiableWALSegment modifiableWALSegment =
        new ModifiableWALSegment(testDirectory.resolve("seg.wal"), 1024, 42)) {

      final TestRecord testRecordOne = new TestRecord(random, 128, 64);
      final TestRecord testRecordTwo = new TestRecord(random, 128, 64);

      final Optional<OLogSequenceNumber> oLsnOne = modifiableWALSegment.write(testRecordOne);
      Assert.assertTrue(oLsnOne.isPresent());
      final OLogSequenceNumber lsnOne = oLsnOne.get();

      Assert.assertEquals(lsnOne, testRecordOne.getLsn());

      final Optional<OLogSequenceNumber> oLsnTwo = modifiableWALSegment.write(testRecordTwo);
      Assert.assertTrue(oLsnTwo.isPresent());

      final OLogSequenceNumber lsnTwo = oLsnTwo.get();
      Assert.assertEquals(lsnTwo, testRecordTwo.getLsn());

      Assert.assertTrue(lsnTwo.compareTo(lsnOne) > 0);

      Optional<WriteableWALRecord> oRecordOne = modifiableWALSegment.read(lsnOne);
      Assert.assertTrue(oRecordOne.isPresent());

      verifyAdditionTwoRecords(
          modifiableWALSegment, testRecordOne, testRecordTwo, lsnOne, lsnTwo, oRecordOne);

      modifiableWALSegment.sync();

      verifyAdditionTwoRecords(
          modifiableWALSegment, testRecordOne, testRecordTwo, lsnOne, lsnTwo, oRecordOne);

      modifiableWALSegment.sync();

      verifyAdditionTwoRecords(
          modifiableWALSegment, testRecordOne, testRecordTwo, lsnOne, lsnTwo, oRecordOne);
    }
  }

  private void verifyAdditionTwoRecords(
      ModifiableWALSegment modifiableWALSegment,
      TestRecord testRecordOne,
      TestRecord testRecordTwo,
      OLogSequenceNumber lsnOne,
      OLogSequenceNumber lsnTwo,
      @SuppressWarnings("OptionalUsedAsFieldOrParameterType")
          Optional<WriteableWALRecord> oRecordOne) {
    Assert.assertTrue(oRecordOne.isPresent());
    TestRecord recordOne = (TestRecord) oRecordOne.get();
    Assert.assertEquals(lsnOne, recordOne.getLsn());
    Assert.assertArrayEquals(testRecordOne.data, recordOne.data);

    Optional<WriteableWALRecord> oRecordTwo = modifiableWALSegment.read(lsnTwo);
    Assert.assertTrue(oRecordTwo.isPresent());

    TestRecord recordTwo = (TestRecord) oRecordTwo.get();
    Assert.assertEquals(lsnTwo, recordTwo.getLsn());
    Assert.assertArrayEquals(testRecordTwo.data, recordTwo.data);

    oRecordTwo = modifiableWALSegment.next(lsnOne);
    Assert.assertTrue(oRecordTwo.isPresent());

    recordTwo = (TestRecord) oRecordTwo.get();
    Assert.assertEquals(lsnTwo, recordTwo.getLsn());
    Assert.assertArrayEquals(testRecordTwo.data, recordTwo.data);

    oRecordTwo = modifiableWALSegment.next(recordOne);
    Assert.assertTrue(oRecordTwo.isPresent());

    recordTwo = (TestRecord) oRecordTwo.get();
    Assert.assertEquals(lsnTwo, recordTwo.getLsn());
    Assert.assertArrayEquals(testRecordTwo.data, recordTwo.data);
  }

  @Test
  public void testAdditionTillFull() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testAdditionTillFull seed : " + seed);
    final Random random = new Random(seed);

    final List<TestRecord> writtenRecords = new ArrayList<>();
    try (final ModifiableWALSegment modifiableWALSegment =
        new ModifiableWALSegment(testDirectory.resolve("seg.wal"), 1024, 42)) {

      Optional<OLogSequenceNumber> begin = modifiableWALSegment.begin();
      Optional<OLogSequenceNumber> end = modifiableWALSegment.end();

      Assert.assertFalse(begin.isPresent());
      Assert.assertFalse(end.isPresent());

      OLogSequenceNumber prevLsn = null;
      TestRecord testRecord = new TestRecord(random, 128, 64);
      Optional<OLogSequenceNumber> oLsn = modifiableWALSegment.write(testRecord);

      while (oLsn.isPresent()) {
        final OLogSequenceNumber lsn = oLsn.get();
        Assert.assertEquals(lsn, testRecord.getLsn());

        if (prevLsn != null) {
          Assert.assertTrue(lsn.compareTo(prevLsn) > 0);
        }

        prevLsn = lsn;
        writtenRecords.add(testRecord);

        begin = modifiableWALSegment.begin();
        Assert.assertTrue(begin.isPresent());

        end = modifiableWALSegment.end();
        Assert.assertTrue(end.isPresent());

        Assert.assertEquals(writtenRecords.get(0).getLsn(), begin.get());
        Assert.assertEquals(writtenRecords.get(writtenRecords.size() - 1).getLsn(), end.get());

        testRecord = new TestRecord(random, 128, 64);
        oLsn = modifiableWALSegment.write(testRecord);
      }

      verifyAdditionTillFull(writtenRecords, modifiableWALSegment);

      modifiableWALSegment.sync();

      verifyAdditionTillFull(writtenRecords, modifiableWALSegment);

      modifiableWALSegment.sync();

      verifyAdditionTillFull(writtenRecords, modifiableWALSegment);
    }
  }

  private void verifyAdditionTillFull(
      List<TestRecord> writtenRecords, ModifiableWALSegment modifiableWALSegment) {
    Optional<OLogSequenceNumber> begin;
    Optional<OLogSequenceNumber> end;
    for (final TestRecord tRecord : writtenRecords) {
      final Optional<WriteableWALRecord> wRecord = modifiableWALSegment.read(tRecord.getLsn());
      Assert.assertTrue(wRecord.isPresent());

      final TestRecord lRecord = (TestRecord) wRecord.get();
      Assert.assertArrayEquals(tRecord.data, lRecord.data);
      Assert.assertEquals(tRecord.getLsn(), lRecord.getLsn());
    }

    for (int i = 0; i < writtenRecords.size(); i++) {
      TestRecord tRecordOrigin = writtenRecords.get(i);

      for (TestRecord tRecord : writtenRecords.subList(i + 1, writtenRecords.size())) {
        final Optional<WriteableWALRecord> wRecord = modifiableWALSegment.next(tRecordOrigin);

        Assert.assertTrue(wRecord.isPresent());
        final TestRecord lRecord = (TestRecord) wRecord.get();
        Assert.assertArrayEquals(tRecord.data, lRecord.data);
        Assert.assertEquals(tRecord.getLsn(), lRecord.getLsn());

        tRecordOrigin = tRecord;
      }

      final Optional<WriteableWALRecord> wRecord = modifiableWALSegment.next(tRecordOrigin);
      Assert.assertFalse(wRecord.isPresent());
    }

    for (int i = 0; i < writtenRecords.size(); i++) {
      TestRecord tRecordOrigin = writtenRecords.get(i);

      for (TestRecord tRecord : writtenRecords.subList(i + 1, writtenRecords.size())) {
        final Optional<WriteableWALRecord> wRecord =
            modifiableWALSegment.next(tRecordOrigin.getLsn());

        Assert.assertTrue(wRecord.isPresent());
        final TestRecord lRecord = (TestRecord) wRecord.get();
        Assert.assertArrayEquals(tRecord.data, lRecord.data);
        Assert.assertEquals(tRecord.getLsn(), lRecord.getLsn());

        tRecordOrigin = tRecord;
      }

      final Optional<WriteableWALRecord> wRecord = modifiableWALSegment.next(tRecordOrigin);
      Assert.assertFalse(wRecord.isPresent());
    }

    begin = modifiableWALSegment.begin();
    Assert.assertTrue(begin.isPresent());

    end = modifiableWALSegment.end();
    Assert.assertTrue(end.isPresent());

    Assert.assertEquals(writtenRecords.get(0).getLsn(), begin.get());
    Assert.assertEquals(writtenRecords.get(writtenRecords.size() - 1).getLsn(), end.get());
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
