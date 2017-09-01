package com.orientechnologies.orient.core.storage.impl.local.paginated.wal;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 * Created by tglman on 20/01/16.
 */
public class OLogSegmentV2Test {

  @Test
  public void testLogGenerator() throws IOException {

    OLogSegmentV2.OLogRecord res = OLogSegmentV2.generateLogRecord(0, new byte[10]);
    assertEquals(OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPageV2.RECORDS_OFFSET + OWALPageV2.calculateSerializedSize(10), res.writeTo);

    //Just fit in
    res = OLogSegmentV2.generateLogRecord(0, new byte[OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE)]);
    assertEquals(OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPageV2.RECORDS_OFFSET + OWALPageV2.MAX_ENTRY_SIZE, res.writeTo);

    //it just goes out
    res = OLogSegmentV2.generateLogRecord(0, new byte[OWALPageV2.MAX_ENTRY_SIZE]);
    assertEquals(OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPageV2.RECORDS_OFFSET * 2 + OWALPageV2.calculateSerializedSize(OWALPageV2.MAX_ENTRY_SIZE) + OWALPageV2
        .calculateSerializedSize(0), res.writeTo);

    //it not fit because start from somewhere in the page
    res = OLogSegmentV2.generateLogRecord(50, new byte[OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE)]);
    assertEquals(50, res.writeFrom);
    assertEquals(50 + OWALPageV2.RECORDS_OFFSET + OWALPageV2.MAX_ENTRY_SIZE + OWALPageV2.calculateSerializedSize(0), res.writeTo);

    //it start from the end of the page it fit in the next one
    res = OLogSegmentV2
        .generateLogRecord(OWALPage.PAGE_SIZE - 1, new byte[OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE)]);
    assertEquals(OWALPage.PAGE_SIZE + OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE * 2, res.writeTo);

    //same as before but full page
    res = OLogSegmentV2.generateLogRecord(OWALPage.PAGE_SIZE, new byte[OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE)]);
    assertEquals(OWALPage.PAGE_SIZE + OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE + OWALPageV2.RECORDS_OFFSET + OWALPageV2.MAX_ENTRY_SIZE, res.writeTo);

    //Just filled plus a byte
    res = OLogSegmentV2.generateLogRecord(OWALPage.PAGE_SIZE + OWALPageV2.RECORDS_OFFSET + 1, new byte[10]);
    assertEquals(OWALPage.PAGE_SIZE + OWALPageV2.RECORDS_OFFSET + 1, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE + OWALPageV2.RECORDS_OFFSET + 1 + OWALPageV2.calculateSerializedSize(10), res.writeTo);

    // multipage
    long starting = OWALPage.PAGE_SIZE + 50;
    int contentSize = OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE) * 3;
    res = OLogSegmentV2.generateLogRecord(starting, new byte[contentSize]);
    assertEquals(starting, res.writeFrom);
    //include the starting the base offset for three pages the content size and 4 matadata parts because the record is splitted for 4 pages
    assertEquals(starting + 3 * OWALPageV2.RECORDS_OFFSET + contentSize + OWALPageV2.calculateSerializedSize(0) * 4, res.writeTo);

    //fit exactly two pages
    res = OLogSegmentV2.generateLogRecord(0, new byte[OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE) * 2]);
    assertEquals(OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE * 2, res.writeTo);

    res = OLogSegmentV2.generateLogRecord(0, new byte[OWALPageV2.calculateRecordSize(OWALPageV2.MAX_ENTRY_SIZE) * 3]);
    assertEquals(OWALPageV2.RECORDS_OFFSET, res.writeFrom);
    assertEquals(OWALPage.PAGE_SIZE * 3, res.writeTo);

  }

}
