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
import java.util.Collection;
import java.util.Date;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.query.sql.OSQLSynchQuery;
import com.orientechnologies.orient.core.record.impl.ORecordDocument;
import com.orientechnologies.orient.test.database.base.OrientTest;

@Test(groups = "query", sequential = true)
public class SQLQueryTest {
	private ODatabaseDocument	database;
	private ORecordDocument		record;

	@Parameters(value = "url")
	public SQLQueryTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void querySchemaAndLike() {
		database.open("admin", "admin");

		List<ORecordDocument> result = database.query(
				new OSQLSynchQuery<ORecordDocument>("select * from Animal where ID = 10 and name like 'G%'")).execute();

		System.out.println("select * from Animal where ID = 10 and name like 'G%'");

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Animal"));
			Assert.assertEquals(record.field("id"), 10);
			Assert.assertTrue(record.field("name").toString().startsWith("G"));
		}

		database.close();
	}

	@Test
	public void queryColumnOrAndRange() {
		database.open("admin", "admin");

		List<ORecordDocument> result = database.query(
				new OSQLSynchQuery<ORecordDocument>("SELECT * FROM animal WHERE column(0) < 5 OR column(0) >= 3 AND column(5) < 7"))
				.execute();

		System.out.println("SELECT * FROM animal WHERE column(0) < 5 OR column(0) >= 3 AND column(5) < 7");

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Animal"));
			Assert.assertTrue((Integer) record.field("id") < 5 || (Integer) record.field("id") >= 3 && (Float) record.field("id") < 7);
		}

		database.close();
	}

	@Test
	public void queryColumnAndOrRange() {
		database.open("admin", "admin");

		List<ORecordDocument> result = database.query(
				new OSQLSynchQuery<ORecordDocument>("SELECT * FROM animal WHERE column(0) < 5 AND column(0) >= 3 OR column(5) <= 403"))
				.execute();

		System.out.println("SELECT * FROM animal WHERE column(0) < 5 AND column(0) >= 3 OR column(5) <= 403");

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Animal"));
			Assert.assertTrue((Integer) record.field("id") < 5 && (Integer) record.field("id") >= 3
					|| (Float) record.field("price") <= 403);
		}

		database.close();
	}

	@Test
	public void queryLogicalCluster() throws ParseException {
		database.open("admin", "admin");

		String rangeFrom = "2009-09-30";
		String rangeTo = "2009-11-01";

		Date rangeFromDate = database.getStorage().getConfiguration().getDateFormatInstance().parse(rangeFrom);
		Date rangeToDate = database.getStorage().getConfiguration().getDateFormatInstance().parse(rangeTo);

		List<ORecordDocument> result = database.query(
				new OSQLSynchQuery<ORecordDocument>("select * from Order where date > '" + rangeFrom + "' and date < '" + rangeTo + "'"))
				.execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("order"));
			Assert.assertTrue(((Date) record.field("date")).after(rangeFromDate));
			Assert.assertTrue(((Date) record.field("date")).before(rangeToDate));
			Assert.assertTrue(record.field("name").toString().startsWith("G"));
		}

		database.close();
	}

	@Test
	public void queryCollectionContainsLowerCaseSubStringIgnoreCase() {
		database.open("admin", "admin");

		List<ORecordDocument> result = database.query(
				new OSQLSynchQuery<ORecordDocument>(
						"select * from animaltype where races contains (name.toLowerCase().subString(0,1) = 'e')")).execute();

		System.out.println("select * from animaltype where races contains (name.toLowercase().substring(0,1) = 'e')");

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animaltype"));
			Assert.assertNotNull(record.field("races"));

			Collection<ORecordDocument> races = record.field("races");
			boolean found = false;
			for (ORecordDocument race : races) {
				if (((String) race.field("name")).toLowerCase().substring(0, 1).equals("e")) {
					found = true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		database.close();
	}

	@Test
	public void queryCollectionContainsIn() {
		database.open("admin", "admin");

		List<ORecordDocument> result = database.query(
				new OSQLSynchQuery<ORecordDocument>("select * from animaltype where races contains (name in ['European','Asiatic'])"))
				.execute();

		System.out.println("select * from animaltype where races contains (name in ['European','Asiatic'])");

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animaltype"));
			Assert.assertNotNull(record.field("races"));

			Collection<ORecordDocument> races = record.field("races");
			boolean found = false;
			for (ORecordDocument race : races) {
				if (((String) race.field("name")).equals("European") || ((String) race.field("name")).equals("Asiatic")) {
					found = true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		database.close();
	}
}
