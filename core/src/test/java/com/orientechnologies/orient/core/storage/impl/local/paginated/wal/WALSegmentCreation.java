package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.storage.impl.local.paginated.OLocalPaginatedStorage;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 24/12/14
 */
@Test(enabled = false)
public class WALSegmentCreation {
  private ODiskWriteAheadLog     writeAheadLog;
  private File                   testDir;
  private volatile boolean       stop = false;

  private ExecutorService        writerExecutor;
  private OLocalPaginatedStorage localPaginatedStorage;

  @BeforeClass(enabled = false)
  public void beforeClass() throws Exception {
    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 129, TestRecordTwo.class);
    OWALRecordsFactory.INSTANCE.registerNewRecord((byte) 128, TestRecordOne.class);

    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty())
      buildDirectory = ".";

    testDir = new File(buildDirectory, "WALSegmentCreationTest");
    if (!testDir.exists())
      testDir.mkdir();

    localPaginatedStorage = mock(OLocalPaginatedStorage.class);
    when(localPaginatedStorage.getStoragePath()).thenReturn(testDir.getAbsolutePath());
    when(localPaginatedStorage.getName()).thenReturn("WALSegmentCreationTest");

    writeAheadLog = new ODiskWriteAheadLog(400, 500, 64 * 1024L * 1024L, null, localPaginatedStorage);

    writerExecutor = Executors.newCachedThreadPool();
  }

  public void testLogSegmentCreation() throws Exception {
    List<Future<Void>> futures = new ArrayList<Future<Void>>();

    for (int i = 0; i < 8; i++) {
      futures.add(writerExecutor.submit(new Writer()));
    }

    Thread.sleep(15 * 60 * 1000);

    stop = true;

    for (Future<Void> future : futures)
      future.get();

    final Set<OOperationUnitId> operations = new HashSet<OOperationUnitId>();
    writeAheadLog.close();

    writeAheadLog = new ODiskWriteAheadLog(200, 500, 64 * 1024L * 1024L, null, localPaginatedStorage);

    OLogSequenceNumber lsn = writeAheadLog.begin();
    long segment = lsn.getSegment();

    System.out.println("Segment : " + lsn.getSegment());
    while (lsn != null) {
      OWALRecord record = writeAheadLog.read(lsn);
      if (record instanceof OAtomicUnitStartRecord) {
        OOperationUnitId operationUnitId = ((OAtomicUnitStartRecord) record).getOperationUnitId();
        Assert.assertFalse(operations.contains(operationUnitId));
        operations.add(operationUnitId);
      } else if (record instanceof OAtomicUnitEndRecord) {
        OOperationUnitId operationUnitId = ((OAtomicUnitEndRecord) record).getOperationUnitId();
        Assert.assertTrue(operations.contains(operationUnitId));
        operations.remove(operationUnitId);
      } else if (record instanceof OOperationUnitRecord) {
        OOperationUnitId operationUnitId = ((OOperationUnitRecord) record).getOperationUnitId();
        Assert.assertTrue(operations.contains(operationUnitId));
      }

      lsn = writeAheadLog.next(lsn);
      if (lsn != null && lsn.getSegment() != segment) {
        System.out.println("Segment : " + lsn.getSegment());
        Assert.assertTrue(operations.isEmpty());
        segment = lsn.getSegment();
      }
    }

    Assert.assertTrue(operations.isEmpty());
  }

  public class Writer implements Callable<Void> {

    @Override
    public Void call() throws Exception {

      while (!stop) {
        OOperationUnitId operationUnitId = OOperationUnitId.generateId();
        writeAheadLog.logAtomicOperationStartRecord(true, operationUnitId);
        writeAheadLog.log(new TestRecordOne(100, operationUnitId));
        writeAheadLog.log(new TestRecordOne(200, operationUnitId));
        writeAheadLog.log(new TestRecordTwo(100));
        writeAheadLog.log(new TestRecordOne(300, operationUnitId));
        writeAheadLog.log(new TestRecordOne(200, operationUnitId));
        writeAheadLog.log(new TestRecordTwo(200));
        writeAheadLog.log(new TestRecordOne(100, operationUnitId));
        writeAheadLog.logAtomicOperationEndRecord(operationUnitId, false, new OLogSequenceNumber(0, 0), null);
        writeAheadLog.log(new TestRecordTwo(100));
      }

      return null;
    }
  }

  public static final class TestRecordOne extends OOperationUnitRecord {
    private byte[] data;

    public TestRecordOne() {
    }

    public TestRecordOne(int size, OOperationUnitId operationUnitId) {
      super(operationUnitId);
      Random random = new Random();
      data = new byte[size - OIntegerSerializer.INT_SIZE - (OIntegerSerializer.INT_SIZE + 3) - 1];
      random.nextBytes(data);
    }

    @Override
    public int toStream(byte[] content, int offset) {
      offset = super.toStream(content, offset);
      OIntegerSerializer.INSTANCE.serializeNative(data.length, content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      System.arraycopy(data, 0, content, offset, data.length);
      offset += data.length;

      return offset;
    }

    @Override
    public int fromStream(byte[] content, int offset) {
      offset = super.fromStream(content, offset);
      int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[size];
      System.arraycopy(content, offset, data, 0, data.length);
      offset += size;

      return offset;
    }

    @Override
    public int serializedSize() {
      return super.serializedSize() + OIntegerSerializer.INT_SIZE + data.length + 1;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      TestRecordTwo that = (TestRecordTwo) o;

      if (!Arrays.equals(data, that.data))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
      return "TestRecordOne {size: " + (data.length + OIntegerSerializer.INT_SIZE + 1 + (OIntegerSerializer.INT_SIZE + 3) + "}");
    }
  }

  public static final class TestRecordTwo extends OAbstractWALRecord {
    private byte[] data;

    public TestRecordTwo() {
    }

    public TestRecordTwo(int size) {
      Random random = new Random();
      data = new byte[size - OIntegerSerializer.INT_SIZE - (OIntegerSerializer.INT_SIZE + 3) - 1];
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
      int size = OIntegerSerializer.INSTANCE.deserializeNative(content, offset);
      offset += OIntegerSerializer.INT_SIZE;

      data = new byte[size];
      System.arraycopy(content, offset, data, 0, data.length);
      offset += size;

      return offset;
    }

    @Override
    public int serializedSize() {
      return OIntegerSerializer.INT_SIZE + data.length + 1;
    }

    @Override
    public boolean isUpdateMasterRecord() {
      return false;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o)
        return true;
      if (o == null || getClass() != o.getClass())
        return false;

      TestRecordTwo that = (TestRecordTwo) o;

      if (!Arrays.equals(data, that.data))
        return false;

      return true;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(data);
    }

    @Override
    public String toString() {
      return "TestRecordTwo {size: " + (data.length + OIntegerSerializer.INT_SIZE + 1 + (OIntegerSerializer.INT_SIZE + 3) + "}");
    }
  }
}