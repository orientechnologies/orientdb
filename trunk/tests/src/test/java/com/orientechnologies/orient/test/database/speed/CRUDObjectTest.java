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
package com.orientechnologies.orient.test.database.speed;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.test.domain.animal.Animal;

@Test(groups = { "crud", "record-object" }, sequential = true)
public class CRUDObjectTest {

	private ODatabaseObject	database;

	protected long					startRecordNumber;

	@Parameters(value = "url")
	public CRUDObjectTest(String iURL) {
		database = null;// new ODatabaseVObjectTx(iURL);
	}

	public void create() {
		database.open("admin", "admin");

		startRecordNumber = database.countClass("Animal");

		Animal object = new Animal();

		object.setId(2222);
		object.setName("Gipsy");
		object.setType("Cat");
		object.setRace("European");
		object.setLocation("Italy");
		object.setPrice(300f);

		database.save(object);

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClass("Animal") - startRecordNumber, 1);

		database.close();
	}
}
