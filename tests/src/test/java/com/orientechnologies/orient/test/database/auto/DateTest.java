/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.util.ODateHelper;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

@Test(groups = "sql-select")
public class DateTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public DateTest(@Optional String url) {
    super(url);
  }

  @Test
  public void testDateConversion() throws ParseException {
    final long begin = System.currentTimeMillis();

    ODocument doc1 = new ODocument("Order");
    doc1.field("context", "test");
    doc1.field("date", new Date());
    doc1.save();

    ODocument doc2 = new ODocument("Order");
    doc2.field("context", "test");
    doc2.field("date", System.currentTimeMillis());
    doc2.save();

    doc2.reload();
    Assert.assertTrue(doc2.field("date", OType.DATE) instanceof Date);

    doc2.reload();
    Assert.assertTrue(doc2.field("date", Date.class) instanceof Date);

    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select * from Order where date >= ? and context = 'test'")).execute(begin);

    Assert.assertEquals(result.size(), 2);
  }

  @Test
  public void testDatePrecision() throws ParseException {
    final long begin = System.currentTimeMillis();

    String dateAsString = database.getStorage().getConfiguration().getDateFormatInstance().format(begin);

    ODocument doc = new ODocument("Order");
    doc.field("context", "testPrecision");
    doc.field("date", ODateHelper.now(), OType.DATETIME);
    doc.save();

    List<ODocument> result = database.command(
        new OSQLSynchQuery<ODocument>("select * from Order where date >= ? and context = 'testPrecision'")).execute(dateAsString);

    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testDateTypes() throws ParseException {
    ODocument doc = new ODocument();
    doc.field("context", "test");
    doc.field("date", System.currentTimeMillis(), OType.DATE);

    Assert.assertTrue(doc.field("date") instanceof Date);
  }

  /**
   * https://github.com/orientechnologies/orientjs/issues/48
   */
  @Test
  public void testDateGregorianCalendar() throws ParseException {
    database.command(new OCommandSQL("CREATE CLASS TimeTest EXTENDS V")).execute();

    final SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    final Date date = df.parse("1200-11-11 00:00:00.000");

    database.command(new OCommandSQL("CREATE VERTEX TimeTest SET firstname = ?, birthDate = ?")).execute("Robert", date);

    final List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from TimeTest where firstname = ?"), "Robert");
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).field("birthDate"), date);
  }
}
