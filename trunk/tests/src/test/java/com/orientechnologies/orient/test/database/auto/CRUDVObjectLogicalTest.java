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

import com.orientechnologies.orient.core.db.vobject.ODatabaseVObject;
import com.orientechnologies.orient.core.db.vobject.ODatabaseVObjectTx;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
@SuppressWarnings("unchecked")
public class CRUDVObjectLogicalTest {

	private ODatabaseVObject	database;
	private ORecordVObject		record;

	protected long						startRecordNumber;

	@Parameters(value = "url")
	public CRUDVObjectLogicalTest(String iURL) {
		database = new ODatabaseVObjectTx(iURL);
		record = database.newInstance();
	}

	public void create() {
		database.open("admin", "admin");

		startRecordNumber = database.countClass("AnimalType");

		record.reset();
		record.setClassName("AnimalType");
		record.field("name", "Cat");

		Set<ORecordVObject> races = new HashSet<ORecordVObject>();
		races.add((ORecordVObject) database.newInstance("AnimalRace").field("name", "European"));
		races.add((ORecordVObject) database.newInstance("AnimalRace").field("name", "Siamese"));
		record.field("races", races);

		record.save();

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClass("AnimalType") - startRecordNumber, 1);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void read() {
		database.open("admin", "admin");

		// LOAD THE LAST ENTRY JUST CREATED
		record = database.browseClass("AnimalType").last().previous();

		Assert.assertEquals(record.field("name"), "Cat");
		Assert.assertTrue(((List<ORecordVObject>) record.field("races")).size() == 2);

		database.close();
	}

	@Test(dependsOnMethods = "read")
	public void update() {
		database.open("admin", "admin");

		record.reset();

		record = database.browseClass("AnimalType").last().previous();

		List<ORecordVObject> races = record.field("races");
		races.add((ORecordVObject) database.newInstance("AnimalRace").field("name", "Egyptian"));
		record.setDirty();

		record.save();

		database.close();
	}

	@Test(dependsOnMethods = "update")
	public void testUpdate() {
		database.open("admin", "admin");

		record.reset();

		record = database.browseClass("AnimalType").last().previous();

		Assert.assertEquals(record.field("name"), "Cat");
		Assert.assertTrue(((List<ORecordVObject>) record.field("races")).size() == 3);

		database.close();
	}

	@Test(dependsOnMethods = "testUpdate")
	public void delete() {
		database.open("admin", "admin");

		startRecordNumber = database.countClass("AnimalType");

		record = database.browseClass("AnimalType").last().previous();
		record.delete();

		Assert.assertEquals(database.countClass("AnimalType"), startRecordNumber - 1);

		database.close();
	}
}
