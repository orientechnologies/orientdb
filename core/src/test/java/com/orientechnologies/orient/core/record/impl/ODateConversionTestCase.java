/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.record.impl;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
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
    try (OrientDB ctx = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      ctx.execute("create database test memory users(admin identified by 'adminpwd' role admin)");
      try (ODatabaseDocument db = ctx.open("test", "admin", "adminpwd")) {

        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        format.setTimeZone(TimeZone.getTimeZone("GMT"));
        Date date = format.parse("2016-08-31 23:30:00");

        db.set(ODatabase.ATTRIBUTES.TIMEZONE, "GMT");

        ODocument doc = new ODocument();

        doc.field("dateTime", date);
        String formatted = doc.field("dateTime.format('yyyy-MM-dd')");

        Assert.assertEquals("2016-08-31", formatted);
      }
      ctx.drop("test");
    }
  }
}
