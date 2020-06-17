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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.record.binary.ORecordSerializerBinary;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.TimeZone;
import org.junit.Assert;
import org.junit.Test;

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
    byte[] res = serializer.toStream(document);
    ODocument extr = (ODocument) serializer.fromStream(res, new ODocument(), new String[] {});
    final String[] fields = extr.fieldNames();

    assertNotNull(fields);
    assertEquals(fields.length, 1);
    assertEquals(fields[0], "date");

    Date old = document.field("date");
    Date newDate = extr.field("date");
    Calendar cal = Calendar.getInstance();
    cal.setTime(old);
    Calendar cal1 = Calendar.getInstance();
    cal1.setTime(old);
    assertEquals(cal.get(Calendar.YEAR), cal1.get(Calendar.YEAR));
    assertEquals(cal.get(Calendar.MONTH), cal1.get(Calendar.MONTH));
    assertEquals(cal.get(Calendar.DAY_OF_MONTH), cal1.get(Calendar.DAY_OF_MONTH));
  }

  @Test
  public void testDateFormantWithMethod() throws ParseException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:format");
    db.create();
    try {

      SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
      format.setTimeZone(TimeZone.getTimeZone("GMT"));
      Date date = format.parse("2016-08-31 23:30:00");

      db.setInternal(ODatabase.ATTRIBUTES.TIMEZONE, "GMT");

      ODocument doc = new ODocument();

      doc.field("dateTime", date);
      String formatted = doc.field("dateTime.format('yyyy-MM-dd')");

      Assert.assertEquals("2016-08-31", formatted);

    } finally {
      db.drop();
    }
  }
}
