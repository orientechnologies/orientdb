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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-update", sequential = true)
public class SQLUpdateTest {
	private ODatabaseDocument	database;
	private int								updatedRecords;

	@Parameters(value = "url")
	public SQLUpdateTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void updateWithWhereOperator() {
		database.open("admin", "admin");

		Integer records = (Integer) database.command(
				new OCommandSQL("update Profile set salary = 120.30, location = 3:2, salary_cloned = salary where surname = 'Obama'"))
				.execute();

		Assert.assertEquals(records.intValue(), 3);

		database.close();
	}

	@Test
	public void updateWithWhereRid() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OCommandSQL("select @rid as rid from Profile where surname = 'Obama'")).execute();

		Assert.assertEquals(result.size(), 3);

		Integer records = (Integer) database.command(new OCommandSQL("update Profile set salary = 133.00 where @rid = ?")).execute(
				result.get(0).field("rid"));

		Assert.assertEquals(records.intValue(), 1);

		database.close();
	}

	@Test(dependsOnMethods = "updateWithWhereOperator")
	public void updateCollectionsAddWithWhereOperator() {
		database.open("admin", "admin");

		updatedRecords = (Integer) database.command(new OCommandSQL("update Account add addresses = 12:0")).execute();

		database.close();
	}

	@Test(dependsOnMethods = "updateCollectionsAddWithWhereOperator")
	public void updateCollectionsRemoveWithWhereOperator() {
		database.open("admin", "admin");

		final int records = (Integer) database.command(new OCommandSQL("update Account remove addresses = 12:0")).execute();

		Assert.assertEquals(records, updatedRecords);

		database.close();
	}

	@Test(dependsOnMethods = "updateCollectionsRemoveWithWhereOperator")
	public void updateAllOperator() {
		database.open("admin", "admin");

		Long total = database.countClass("Profile");

		Integer records = (Integer) database.command(new OCommandSQL("update Profile set sex = 'male'")).execute();

		Assert.assertEquals(records.intValue(), total.intValue());

		database.close();
	}

	@Test
	public void updateWithWildcards() {
		database.open("admin", "admin");

		int updated = (Integer) database.command(new OCommandSQL("update Profile set sex = ? where sex = 'male' limit 1")).execute(
				"male");

		Assert.assertEquals(updated, 1);

		database.close();
	}

	@Test
	public void updateWithWildcardsOnSetAndWhere() {

		database.open("admin", "admin");
		ODocument doc = new ODocument(database, "Person");
		doc.field("name", "Raf");
		doc.field("city", "Torino");
		doc.field("gender", "fmale");
		doc.save();
		checkUpdatedDoc(database, "Raf", "Torino", "fmale");

		/* THESE COMMANDS ARE OK */
		OCommandSQL updatecommand = new OCommandSQL("update Person set gender = 'female' where name = 'Raf'");
		database.command(updatecommand).execute("Raf");
		checkUpdatedDoc(database, "Raf", "Torino", "female");

		updatecommand = new OCommandSQL("update Person set city = 'Turin' where name = ?");
		database.command(updatecommand).execute("Raf");
		checkUpdatedDoc(database, "Raf", "Turin", "female");

		updatecommand = new OCommandSQL("update Person set gender = ? where name = 'Raf'");
		database.command(updatecommand).execute("F");
		checkUpdatedDoc(database, "Raf", "Turin", "F");

		updatecommand = new OCommandSQL("update Person set gender = ?, city = ? where name = 'Raf'");
		database.command(updatecommand).execute("FEMALE", "TORINO");
		checkUpdatedDoc(database, "Raf", "TORINO", "FEMALE");

		updatecommand = new OCommandSQL("update Person set gender = ? where name = ?");
		database.command(updatecommand).execute("f", "Raf");
		checkUpdatedDoc(database, "Raf", "TORINO", "f");

		database.close();
	}

	private void checkUpdatedDoc(ODatabaseDocument database, String expectedName, String expectedCity, String expectedGender) {
		List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select * from person"));
		ODocument oDoc = result.get(0);
		Assert.assertEquals(expectedName, oDoc.field("name"));
		Assert.assertEquals(expectedCity, oDoc.field("city"));
		Assert.assertEquals(expectedGender, oDoc.field("gender"));
	}

}
