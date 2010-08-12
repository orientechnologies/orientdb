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
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.database.base.OrientTest;

@Test(groups = "sql-select", sequential = true)
public class SQLSelectTest {
	private ODatabaseDocument	database;
	private ODocument					record;

	@Parameters(value = "url")
	public SQLSelectTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryNoWhere() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile ")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void querySchemaAndLike() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:Animal where ID = 10 and name like 'G%'")).execute();

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

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("SELECT * FROM cluster:animal WHERE column(0) < 5 OR column(0) >= 3 AND column(5) < 7"))
				.execute();

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

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("SELECT * FROM cluster:animal WHERE column(0) < 5 AND column(0) >= 3 OR column(5) <= 403"))
				.execute();

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

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:Order where date > '" + rangeFrom + "' and date < '" + rangeTo + "'"))
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

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>(
						"select * from cluster:animaltype where races contains (name.toLowerCase().subString(0,1) = 'e')")).execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animaltype"));
			Assert.assertNotNull(record.field("races"));

			Collection<ODocument> races = record.field("races");
			boolean found = false;
			for (ODocument race : races) {
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

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select * from cluster:animaltype where races contains (name in ['European','Asiatic'])"))
				.execute();

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("animaltype"));
			Assert.assertNotNull(record.field("races"));

			Collection<ODocument> races = record.field("races");
			boolean found = false;
			for (ODocument race : races) {
				if (((String) race.field("name")).equals("European") || ((String) race.field("name")).equals("Asiatic")) {
					found = true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		database.close();
	}

	@Test
	public void queryAnyOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where any() like 'N%'")).execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));

			boolean found = false;
			for (Object fieldValue : record.fieldValues()) {
				if (fieldValue != null && fieldValue.toString().startsWith("N")) {
					found = true;
					break;
				}
			}
			Assert.assertTrue(found);
		}

		database.close();
	}

	@Test
	public void queryTraverseAnyOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where any() traverse( 0,3 ) ( any().indexOf( 'Navona' ) > -1 )"))
				.execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));
		}

		database.close();
	}

	@Test
	public void queryTraverseInfiniteLevelOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where any() traverse( 0,-1 ) ( any().indexOf( 'Navona' ) > -1 )"))
				.execute();

		Assert.assertTrue(result.size() > 0);

		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));
		}

		database.close();
	}

	@Test
	public void queryAllOperator() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Account where all() is null")).execute();

		Assert.assertTrue(result.size() == 0);

		database.close();
	}

	@Test
	public void queryOrderBy() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile order by name")).execute();

		Assert.assertTrue(result.size() != 0);

		String lastName = null;
		for (ODocument d : result) {
			if (lastName != null)
				Assert.assertTrue(((String) d.field("name")).compareTo(lastName) > 0);
			lastName = d.field("name");
		}

		database.close();
	}

	@Test
	public void queryConditionAndOrderBy() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where name is not null order by name")).execute();

		Assert.assertTrue(result.size() != 0);

		String lastName = null;
		for (ODocument d : result) {
			if (lastName != null)
				Assert.assertTrue(((String) d.field("name")).compareTo(lastName) > 0);
			lastName = d.field("name");
		}

		database.close();
	}

	@Test
	public void queryConditionsAndOrderBy() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select from Profile where name is not null order by name desc, id asc")).execute();

		Assert.assertTrue(result.size() != 0);

		String lastName = null;
		for (ODocument d : result) {
			if (lastName != null)
				Assert.assertTrue(((String) d.field("name")).compareTo(lastName) < 0);
			lastName = d.field("name");
		}

		database.close();
	}

	@Test
	public void queryRecordTargetRid() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from 10:0")).execute();

		Assert.assertTrue(result.size() == 1);

		for (ODocument d : result) {
			Assert.assertTrue(d.getIdentity().toString().equals("10:0"));
		}

		database.close();
	}

	@Test
	public void queryRecordTargetRids() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from [10:0, 10:1]")).execute();

		Assert.assertTrue(result.size() == 2);

		Assert.assertTrue(result.get(0).getIdentity().toString().equals("10:0"));
		Assert.assertTrue(result.get(1).getIdentity().toString().equals("10:1"));

		database.close();
	}

	@Test
	public void queryRecordAttribRid() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @rid = 10:0")).execute();

		Assert.assertTrue(result.size() == 1);

		for (ODocument d : result) {
			Assert.assertTrue(d.getIdentity().toString().equals("10:0"));
		}

		database.close();
	}

	@Test
	public void queryRecordAttribClass() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @class = 'Profile'"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.getClassName().equals("Profile"));
		}

		database.close();
	}

	@Test
	public void queryRecordAttribVersion() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @version > 0")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.getVersion() > 0);
		}

		database.close();
	}

	@Test
	public void queryRecordAttribSize() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @size >= 50")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.toStream().length >= 50);
		}

		database.close();
	}

	@Test
	public void queryRecordAttribType() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select from Profile where @type = 'document'"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryWrongOperator() {
		database.open("admin", "admin");

		try {
			database.query(new OSQLSynchQuery<ODocument>("select from Profile where name like.toLowerCase() '%Jay%'"));
			Assert.assertFalse(true);
		} catch (Exception e) {
			Assert.assertTrue(true);
		}

		database.close();
	}

}
