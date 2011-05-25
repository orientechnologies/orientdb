/*
 * Copyright 1999-2010 Luca Garulli (l.garulli--at--orientechnologies.com)
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

import java.text.ParseException;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class DateTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public DateTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void testDateConversion() throws ParseException {
		database.open("admin", "admin");

		final long begin = System.currentTimeMillis();

		ODocument doc1 = new ODocument(database, "Order");
		doc1.field("context", "test");
		doc1.field("date", new Date());
		doc1.save();

		ODocument doc2 = new ODocument(database, "Order");
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
		database.close();
	}

	@Test
	public void testDatePrecision() throws ParseException {
		database.open("admin", "admin");

		final long begin = System.currentTimeMillis();

		String dateAsString = database.getStorage().getConfiguration().getDateFormatInstance().format(begin);

		ODocument doc = new ODocument(database, "Order");
		doc.field("context", "testPrecision");
		doc.field("date", new Date(), OType.DATE);
		doc.save();

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from Order where date >= ? and context = 'testPrecision'")).execute(dateAsString);

		Assert.assertEquals(result.size(), 1);
		database.close();
	}

	@Test
	public void testDateTypes() throws ParseException {
		ODocument doc = new ODocument();
		doc.field("context", "test");
		doc.field("date", System.currentTimeMillis(), OType.DATE);

		Assert.assertTrue(doc.field("date") instanceof Date);

	}
}
