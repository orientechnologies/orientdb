package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

/**
 * @author Andrey Lomakin
 * @since 29.04.13
 */
@Test
public class WriteAheadLogTest {
  private OWriteAheadLog writeAheadLog;
  private File           testDir;

  @BeforeClass
  public void beforeClass() {
    testDir = new File("writeAheadLogTest");
    if (!testDir.exists())
      testDir.mkdir();
  }

  @BeforeMethod
  public void beforeMethod() throws Exception {
    writeAheadLog = createWAL();
  }

  private OWriteAheadLog createWAL() throws IOException {
    return new OWriteAheadLog(1024, 1000, 2048, 100L * 1024L * 1024L * 1024L, "WriteAheadLogTest", testDir.getAbsolutePath());
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    writeAheadLog.delete();
  }

  @AfterClass
  public void afterClass() {
    File testDir = new File("writeAheadLogTest");
    if (testDir.exists())
      testDir.delete();
  }

  public void testWriteSingleRecord() throws Exception {

    writeAheadLog.logRecord(new OSetPageDataRecord(new byte[] { 1, 2, 3 }, 10, 20, "test"));

    OWALRecord walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    Assert.assertTrue(walRecord instanceof OSetPageDataRecord);

    OSetPageDataRecord setPageDataRecord = (OSetPageDataRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageOffset(), 10);
    Assert.assertEquals(setPageDataRecord.getData(), new byte[] { 1, 2, 3 });
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.readNext(OLogSequenceNumber.BEGIN));

    writeAheadLog.close();
    writeAheadLog = createWAL();

    walRecord = writeAheadLog.read(OLogSequenceNumber.BEGIN);
    Assert.assertEquals(walRecord.getLsn(), OLogSequenceNumber.BEGIN);
    Assert.assertTrue(walRecord instanceof OSetPageDataRecord);

    setPageDataRecord = (OSetPageDataRecord) walRecord;
    Assert.assertEquals(setPageDataRecord.getPageOffset(), 10);
    Assert.assertEquals(setPageDataRecord.getData(), new byte[] { 1, 2, 3 });
    Assert.assertEquals(setPageDataRecord.getPageIndex(), 20);
    Assert.assertEquals(setPageDataRecord.getFileName(), "test");

    Assert.assertNull(writeAheadLog.readNext(OLogSequenceNumber.BEGIN));
  }

  public void testWriteMultipleRecords() throws Exception {
    List<OSetPageDataRecord> writtenRecords = new ArrayList<OSetPageDataRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 1000;
    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    writeAheadLog.close();

    writeAheadLog = createWAL();
    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  public void testAppendMultipleRecordsAfterClose() throws Exception {
    List<OSetPageDataRecord> writtenRecords = new ArrayList<OSetPageDataRecord>();
    Random rnd = new Random();

    final int recordsToWrite = 1000;
    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    writeAheadLog.close();
    writeAheadLog = createWAL();

    for (int i = 0; i < recordsToWrite; i++) {
      byte[] data = new byte[10];
      rnd.nextBytes(data);

      int pageOffset = rnd.nextInt(65536);
      long pageIndex = rnd.nextLong();
      OSetPageDataRecord setPageDataRecord = new OSetPageDataRecord(data, pageOffset, pageIndex, "test");
      writtenRecords.add(setPageDataRecord);

      writeAheadLog.logRecord(setPageDataRecord);
    }

    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));

    writeAheadLog.close();

    writeAheadLog = createWAL();
    assertLogContent(writeAheadLog, writtenRecords);
    assertLogContent(writeAheadLog, writtenRecords.subList(writtenRecords.size() / 2, writtenRecords.size()));
  }

  private void assertLogContent(OWriteAheadLog writeAheadLog, List<OSetPageDataRecord> writtenRecords) throws Exception {
    Iterator<OSetPageDataRecord> iterator = writtenRecords.iterator();

    OWALRecord writtenRecord = iterator.next();
    OWALRecord readRecord = writeAheadLog.read(writtenRecord.getLsn());

    Assert.assertEquals(writtenRecord, readRecord);
    while (iterator.hasNext()) {
      readRecord = writeAheadLog.readNext(readRecord.getLsn());
      writtenRecord = iterator.next();

      Assert.assertEquals(writtenRecord, readRecord);
    }

    Assert.assertNull(writeAheadLog.readNext(readRecord.getLsn()));
  }
}
