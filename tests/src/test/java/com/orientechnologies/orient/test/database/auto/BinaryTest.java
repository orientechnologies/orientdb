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

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class BinaryTest {
	private ODatabaseDocument	database;
	private ORID							rid;

	@Parameters(value = "url")
	public BinaryTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void testMixedCreateEmbedded() {
		database.open("admin", "admin");

		ODocument doc = new ODocument(database);
		doc.field("binary", "Binary data".getBytes());

		doc.save();

		doc.reload();
		Assert.assertEquals(new String((byte[]) doc.field("binary", OType.BINARY)), "Binary data");

		database.close();
	}

	@Test
	public void testBasicCreateExternal() {
		database.open("admin", "admin");

		ORecordBytes record = new ORecordBytes(database, "This is a test".getBytes());
		record.save();
		rid = record.getIdentity();

		database.close();
	}

	@Test(dependsOnMethods = "testBasicCreateExternal")
	public void testBasicReadExternal() {
		database.open("admin", "admin");

		ORecordBytes record = database.load(rid);

		Assert.assertEquals("This is a test", new String(record.toStream()));

		database.close();
	}

	@Test(dependsOnMethods = "testBasicReadExternal")
	public void testMixedCreateExternal() {
		database.open("admin", "admin");

		ODocument doc = new ODocument(database);
		doc.field("binary", new ORecordBytes(database, "Binary data".getBytes()));

		doc.save();
		rid = doc.getIdentity();

		database.close();
	}

	@Test(dependsOnMethods = "testMixedCreateExternal")
	public void testMixedReadExternal() {
		database.open("admin", "admin");

		ODocument doc = new ODocument(database, rid);
		doc.reload();

		Assert.assertEquals("Binary data", new String(((ORecordBytes) doc.field("binary")).toStream()));

		database.close();
	}
}
