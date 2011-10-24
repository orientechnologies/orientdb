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

import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;

@Test(groups = "sql-insert", sequential = true)
public class SQLInsertTest {
	private ODatabaseDocument	database;

	@Parameters(value = "url")
	public SQLInsertTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void insertOperator() {
		database.open("admin", "admin");

		ODocument doc = (ODocument) database.command(
				new OCommandSQL("insert into Profile (name, surname, salary, location, dummy) values ('Luca','Smith', 109.9, 13:3, name)"))
				.execute();

		Assert.assertTrue(doc != null);

		database.close();
	}

	@Test
	public void insertWithWildcards() {
		database.open("admin", "admin");

		ODocument doc = (ODocument) database.command(
				new OCommandSQL("insert into Profile (name, surname, salary, location, dummy) values (?,?,?,?,?)")).execute("Marc",
				"Smith", 120.0, new ORecordId(13, 3), "hooray");

		Assert.assertTrue(doc != null);
		Assert.assertEquals(doc.field("name"), "Marc");
		Assert.assertEquals(doc.field("surname"), "Smith");
		Assert.assertEquals(((Number) doc.field("salary")).floatValue(), 120.0f);
		Assert.assertEquals(doc.field("location", OType.LINK), new ORecordId(13, 3));
		Assert.assertEquals(doc.field("dummy"), "hooray");

		database.close();
	}

	@Test
	public void insertMap() {
		database.open("admin", "admin");

		ODocument doc = (ODocument) database
				.command(
						new OCommandSQL(
								"insert into cluster:default (equaledges, name, properties) values ('no', 'circle', {'round':false, 'blaaa':'zigzag'} )"))
				.execute();

		Assert.assertTrue(doc != null);

		doc = (ODocument) new ODocument(database, doc.getIdentity()).load();

		Assert.assertEquals(doc.field("equaledges"), "no");
		Assert.assertEquals(doc.field("name"), "circle");
		Assert.assertTrue(doc.field("properties") instanceof Map);

		Map<Object, Object> entries = ((Map<Object, Object>) doc.field("properties"));
		Assert.assertEquals(entries.size(), 2);

		Assert.assertFalse((Boolean) entries.get("round"));
		Assert.assertEquals(entries.get("blaaa"), "zigzag");

		database.close();
	}

	@Test
	public void insertWithNoSpaces() {
		database.open("admin", "admin");

		ODocument doc = (ODocument) database.command(
				new OCommandSQL("insert into cluster:default(id, title)values(10, 'NoSQL movement')")).execute();

		Assert.assertTrue(doc != null);

		database.close();
	}
}
