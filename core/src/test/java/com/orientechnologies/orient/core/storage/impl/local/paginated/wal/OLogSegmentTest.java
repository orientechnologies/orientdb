package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 20/01/16.
 */
public class OLogSegmentTest {

  @Test
  public void testLogGenerator() throws IOException {

    OLogSegment.OLogRecord res = OLogSegment.generateLogRecord(0, new byte[10]);
    assertEquals(OWALPage.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.RECORDS_OFFSET + OWALPage.calculateSerializedSize(10), res.writeTo);

    //Just fit in
    res = OLogSegment.generateLogRecord(0, new byte[OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE)]);
    assertEquals(OWALPage.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.RECORDS_OFFSET + OWALPage.MAX_ENTRY_SIZE, res.writeTo);

    //it just goes out
    res = OLogSegment.generateLogRecord(0, new byte[OWALPage.MAX_ENTRY_SIZE]);
    assertEquals(OWALPage.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.RECORDS_OFFSET * 2 + OWALPage.calculateSerializedSize(OWALPage.MAX_ENTRY_SIZE) + OWALPage.calculateSerializedSize(0), res.writeTo);

    //it not fit becasue start from somewhere in the page
    res = OLogSegment.generateLogRecord(50, new byte[OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE)]);
    assertEquals(50, res.writeFrom);
    assertEquals(50 + OWALPage.RECORDS_OFFSET + OWALPage.MAX_ENTRY_SIZE + OWALPage.calculateSerializedSize(0), res.writeTo);

    //it start from the end of the page it fit in the next one
    res = OLogSegment.generateLogRecord(OWALPage.PAGE_SIZE - 1, new byte[OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE)]);
    assertEquals(OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE * 2, res.writeTo);

    //same as before but full page
    res = OLogSegment.generateLogRecord(OWALPage.PAGE_SIZE, new byte[OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE)]);
    assertEquals(OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET + OWALPage.MAX_ENTRY_SIZE, res.writeTo);

    //Just filled plus a byte
    res = OLogSegment.generateLogRecord(OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET + 1, new byte[10]);
    assertEquals(OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET + 1, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE + OWALPage.RECORDS_OFFSET + 1 + OWALPage.calculateSerializedSize(10), res.writeTo);

    // multipage
    long starting = OWALPage.PAGE_SIZE + 50;
    int contentSize = OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE) * 3;
    res = OLogSegment.generateLogRecord(starting, new byte[contentSize]);
    assertEquals(starting, res.writeFrom);
    //include the starting the base offset for three pages the content size and 4 matadata parts because the record is splitted for 4 pages
    assertEquals(starting + 3 * OWALPage.RECORDS_OFFSET + contentSize + OWALPage.calculateSerializedSize(0) * 4, res.writeTo);


    //fit exactly two pages
    res = OLogSegment.generateLogRecord(0, new byte[OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE) * 2]);
    assertEquals(16, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE * 2, res.writeTo);

    res = OLogSegment.generateLogRecord(0, new byte[OWALPage.calculateRecordSize(OWALPage.MAX_ENTRY_SIZE) * 3]);
    assertEquals(16, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE * 3, res.writeTo);



  }

}
