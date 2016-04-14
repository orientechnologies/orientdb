/*
 * Copyright 2016 OrientDB LTD (info--at--orientdb.com)
 * All Rights Reserved. Commercial License.
 *
 * NOTICE:  All information contained herein is, and remains the property of
 * OrientDB LTD and its suppliers, if any.  The intellectual and
 * technical concepts contained herein are proprietary to
 * OrientDB LTD and its suppliers and may be covered by United
 * Kingdom and Foreign Patents, patents in process, and are protected by trade
 * secret or copyright law.
 *
 * Dissemination of this information or reproduction of this material
 * is strictly forbidden unless prior written permission is obtained
 * from OrientDB LTD.
 *
 * For more information: http://www.orientdb.com
 */

package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import org.junit.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */

public class ODateConversionTestCase {

  private ORecordSerializer serializer = new ORecordSerializerBinary();

  @Test
  public void testDateSerializationWithDST() throws ParseException {

    // write on the db a vertex with a date:
    // 1975-05-31 23:00:00 GMT OR 1975-06-01 01:00:00 (GMT+1) +DST (+2 total)
    SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    Date dateToInsert = format.parse("1975-06-01 01:00:00");

    ODocument document = new ODocument();
    document.field("date", dateToInsert, OType.DATE);
    byte[] res = serializer.toStream(document, false);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    final String[] fields = extr.fieldNames();

    assertNotNull(fields);
    assertEquals(fields.length, 1);
    assertEquals(fields[0], "date");
    // It is correct that is 1 am because this date does not exist at midnight.
    assertEquals(document.field("date"), extr.field("date"));

  }

}
