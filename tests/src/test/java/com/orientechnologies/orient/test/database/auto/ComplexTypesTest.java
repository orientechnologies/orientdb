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

import java.util.ArrayList;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test(groups = { "crud", "record-vobject" })
public class ComplexTypesTest {
	private ODatabaseDocumentTx	database;
	private ODocument						record;
	private String							url;

	@Parameters(value = "url")
	public ComplexTypesTest(final String iURL) {
		url = iURL;
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testEmbeddedList() {
		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument newDoc = new ODocument();

		ArrayList<ODocument> list = new ArrayList<ODocument>();
		newDoc.field("embeddedList", list, OType.EMBEDDEDLIST);
		list.add(new ODocument().field("name", "Luca"));
		list.add(new ODocument(database, "Account").field("name", "Marcus"));

		database.save(newDoc);
		final ORID rid = newDoc.getIdentity();

		database.close();

		database = ODatabaseDocumentPool.global().acquire(url, "admin", "admin");

		ODocument loadedDoc = database.load(rid);
		Assert.assertTrue(loadedDoc.containsField("embeddedList"));
		Assert.assertTrue(loadedDoc.field("embeddedList") instanceof List);
		Assert.assertTrue(((List<ODocument>) loadedDoc.field("embeddedList")).get(0) instanceof ODocument);

		ODocument d = ((List<ODocument>) loadedDoc.field("embeddedList")).get(0);
		Assert.assertEquals(d.field("name"), "Luca");
		d = ((List<ODocument>) loadedDoc.field("embeddedList")).get(1);
		Assert.assertEquals(d.getClassName(), "Account");
		Assert.assertEquals(d.field("name"), "Marcus");

		database.close();
	}
}
