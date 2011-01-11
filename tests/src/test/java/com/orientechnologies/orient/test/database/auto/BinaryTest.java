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

import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ORecordBytes;

public class BinaryTest {
	private ODatabaseDocument	database;
	private ORID							rid;

	@Parameters(value = "url")
	public BinaryTest(String iURL) {
		database = new ODatabaseDocumentTx(iURL);
	}

	@Test
	public void testCreate() {
		database.open("admin", "admin");

		ORecordBytes record = new ORecordBytes(database, "This is a test".getBytes());
//		record.save();
//		rid = record.getIdentity();

		database.close();
	}

	@Test
	public void testRead() {
		database.open("admin", "admin");
//
//		ORecordBytes record = new ORecordBytes(database, rid);
//		record.load();
//
//		Assert.assertEquals("This is a test", new String(record.toStream()));

		database.close();
	}
}
