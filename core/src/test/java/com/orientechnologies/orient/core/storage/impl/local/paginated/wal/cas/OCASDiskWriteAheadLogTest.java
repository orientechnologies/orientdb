package com.orientechnologies.orient.core.storage.impl.local.paginated.wal.cas;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.sql.parser.OInteger;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OAbstractWALRecord;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OLogSequenceNumber;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.OWALRecordsFactory;
import com.orientechnologies.orient.core.storage.impl.local.paginated.wal.WriteAheadLogTest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Locale;
import java.util.Random;

public class OCASDiskWriteAheadLogTest {
  private static Path testDirectory;

  @BeforeClass
  public static void beforeClass() {
    testDirectory = Paths.get(System.getProperty("buildDirectory" + File.separator + "casWALTest",
        "." + File.separator + "target" + File.separator + "casWALTest"));

    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecord.class);
  }

  @Before
  public void before() {
    OFileUtils.deleteRecursively(testDirectory.toFile());
  }

  @Test
  public void testAddSingleRecord() throws Exception {
    final long seed = System.nanoTime();
    System.out.println("testAddSingleRecord : " + seed);
    final Random random = new Random(seed);

    OCASDiskWriteAheadLog wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100,
        Integer.MAX_VALUE, 1000,true, Locale.US);

    TestRecord walRecord = new TestRecord(random);
    final OLogSequenceNumber lsn = wal.log(walRecord);

    List<OWriteableWALRecord> records = wal.read(lsn);
    Assert.assertEquals(1, records.size());
    TestRecord readRecord = (TestRecord) records.get(0);

    Assert.assertArrayEquals(walRecord.data, readRecord.data);
    Assert.assertEquals(lsn, walRecord.getLsn());
    wal.close();

    wal = new OCASDiskWriteAheadLog("walTest", testDirectory, testDirectory, 100, Integer.MAX_VALUE,
        1000, true, Locale.US);

    wal.flush();
    records = wal.read(lsn);
    Assert.assertEquals(2, records.size());
    readRecord = (TestRecord) records.get(0);

    Assert.assertArrayEquals(walRecord.data, readRecord.data);
    Assert.assertEquals(lsn, readRecord.getLsn());
    wal.close();
  }

  public static final class TestRecord extends OAbstractWALRecord {
    private byte[] data;

    public TestRecord() {
    }

    public TestRecord(Random random) {
      int len = random.nextInt(OCASWALPage.PAGE_SIZE) + 1;
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
    public int fromStream(byte[] content, int offset) {
      int len = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[len];
      System.arraycopy(content, offset, data, 0, len);
      offset += len;

      return offset;
    }

    @Override
    public int serializedSize() {
      return data.length + OIntegerSerializer.INT_SIZE;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return false;
    }
  }
}
