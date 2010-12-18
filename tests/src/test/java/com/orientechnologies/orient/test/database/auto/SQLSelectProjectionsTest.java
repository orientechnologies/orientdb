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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;

@Test(groups = "sql-select")
public class SQLSelectProjectionsTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLSelectProjectionsTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void queryProjectionOk() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>(" select nick, followings, followers from Profile "))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionLinkedAndFunction() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select name.toUppercase(), address.city.country.name from Profile")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			if (d.field("name") != null)
				Assert.assertTrue(d.field("name").equals(((String) d.field("name")).toUpperCase()));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionSameFieldTwice() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select name, name.toUppercase() from Profile where name is not null"))
				.execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertNotNull(d.field("name"));
			Assert.assertNotNull(d.field("name2"));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionStaticValues() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select location.city.country.name, address.city.country.name from Profile where location.city.country.name is not null")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {

			Assert.assertNotNull(d.field("location"));
			Assert.assertNull(d.field("address"));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}

	@Test
	public void queryProjectionPrefixAndAppend() {
		database.open("admin", "admin");

		List<ODocument> result = database.command(
				new OSQLSynchQuery<ODocument>("select name.prefix('Mr. ').append('!') from Profile where name is not null")).execute();

		Assert.assertTrue(result.size() != 0);

		for (ODocument d : result) {
			Assert.assertTrue(d.field("name").toString().startsWith("Mr. "));
			Assert.assertTrue(d.field("name").toString().endsWith("!"));

			Assert.assertNull(d.getClassName());
			Assert.assertEquals(d.getRecordType(), ODocument.RECORD_TYPE);
		}

		database.close();
	}
}
