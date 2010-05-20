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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
@SuppressWarnings("unchecked")
public class CRUDDocumentLogicalTest {

	private ODatabaseDocument	database;
	private ODocument					record;

	protected long						startRecordNumber;

	@Parameters(value = "url")
	public CRUDDocumentLogicalTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());
		database = new ODatabaseDocumentTx(iURL);
		record = database.newInstance();
	}

	public void create() {
		database.open("admin", "admin");

		startRecordNumber = database.countClass("City");

		record.reset();
		record.setClassName("City");
		record.field("name", "Cat");

		Set<ODocument> races = new HashSet<ODocument>();
		races.add(database.newInstance("AnimalRace").field("name", "European"));
		races.add(database.newInstance("AnimalRace").field("name", "Siamese"));
		record.field("races", races);

		record.save();

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClass("City") - startRecordNumber, 1);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void read() {
		database.open("admin", "admin");

		// LOAD THE LAST ENTRY JUST CREATED
		record = database.browseClass("City").last().previous();

		Assert.assertEquals(record.field("name"), "Cat");
		Assert.assertTrue(((List<ODocument>) record.field("races")).size() == 2);

		database.close();
	}

	@Test(dependsOnMethods = "read")
	public void update() {
		database.open("admin", "admin");

		record.reset();

		record = database.browseClass("City").last().previous();

		List<ODocument> races = record.field("races");
		races.add(database.newInstance("AnimalRace").field("name", "Egyptian"));
		record.setDirty();

		record.save();

		database.close();
	}

	@Test(dependsOnMethods = "update")
	public void testUpdate() {
		database.open("admin", "admin");

		record.reset();

		record = database.browseClass("City").last().previous();

		Assert.assertEquals(record.field("name"), "Cat");
		Assert.assertTrue(((List<ODocument>) record.field("races")).size() == 3);

		database.close();
	}

	@Test(dependsOnMethods = "testUpdate")
	public void delete() {
		database.open("admin", "admin");

		startRecordNumber = database.countClass("City");

		record = database.browseClass("City").last().previous();
		record.delete();

		Assert.assertEquals(database.countClass("City"), startRecordNumber - 1);

		database.close();
	}
}
