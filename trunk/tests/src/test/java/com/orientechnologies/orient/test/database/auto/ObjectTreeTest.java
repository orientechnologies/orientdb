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

import java.util.Iterator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.query.sql.OSQLSynchQuery;
import com.orientechnologies.orient.core.record.impl.ORecordVObject;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Person;

@Test(groups = { "record-object" }, sequential = true)
public class ObjectTreeTest {
	private ODatabaseObjectTx	database;
	protected long						startRecordNumber;

	@Parameters(value = "url")
	public ObjectTreeTest(String iURL) {
		database = new ODatabaseObjectTx(iURL);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test
	public void testPersonSaving() {
		database.open("admin", "admin");

		Person garibaldi = new Person("Giuseppe", "Garibaldi");
		garibaldi.city = new City("Rome");
		database.save(garibaldi);

		Person bonaparte = new Person("Napoleone", "Bonaparte");
		bonaparte.city = garibaldi.city;
		bonaparte.setParent(garibaldi);
		database.save(bonaparte);

		Assert.assertEquals(database.countClusterElements("Person"), 2);
	}

	@Test(dependsOnMethods = "testPersonSaving")
	public void testCitySaving() {
		Assert.assertEquals(database.countClusterElements("City"), 1);
	}

	@Test(dependsOnMethods = "testCitySaving")
	public void testCityEquality() {
		Iterator<Person> iter = database.browseClass(Person.class);

		Person emp1 = iter.next();
		Person emp2 = iter.next();

		Assert.assertNotSame(emp1, emp2);
		Assert.assertEquals(emp1.city, emp2.city);
	}

	@Test(dependsOnMethods = "testCityEquality")
	public void testSaveCircularLink() {
		Person winston = new Person("Winston", "Churcill");
		winston.city = new City("London");

		Person nicholas = new Person("Nicholas ", "Churcill");
		nicholas.city = winston.city;

		nicholas.setParent(winston);
		winston.setParent(nicholas);

		database.save(nicholas);
	}

	@Test(dependsOnMethods = "testSaveCircularLink")
	public void testQueryCircular() {
		List<ORecordVObject> result = database.getUnderlying().query(new OSQLSynchQuery<ORecordVObject>("select * from Person"))
				.execute();

		ORecordVObject parent;
		for (ORecordVObject r : result) {

			System.out.println(r.field("name") + " " + r.field("surname"));

			parent = r.field("parent");

			if (parent != null)
				System.out.println("- parent: " + parent.field("name") + " " + parent.field("surname"));
		}
	}

	@Test(dependsOnMethods = "testQueryCircular")
	public void testSaveMultiCircular() {
		startRecordNumber = database.countClusterElements("Person");

		Person bObama = new Person("Barack", "Obama");
		bObama.city = new City("Honolulu");
		bObama.addChild(new Person("Malia Ann", "Obama"));
		bObama.addChild(new Person("Natasha", "Obama"));

		database.save(bObama);
	}

	@Test(dependsOnMethods = "testSaveMultiCircular")
	public void testQueryMultiCircular() {
		Assert.assertEquals(database.countClusterElements("Person"), startRecordNumber + 3);

		List<ORecordVObject> result = database.getUnderlying().query(
				new OSQLSynchQuery<ORecordVObject>("select * from Person where name = 'Barack' and surname = 'Obama'")).execute();

		Assert.assertEquals(result.size(), 1);

		ORecordVObject parent;
		List<ORecordVObject> children;
		for (ORecordVObject r : result) {

			System.out.println(r.field("name") + " " + r.field("surname"));

			parent = r.field("parent");

			if (parent != null)
				System.out.println("- parent: " + parent.field("name") + " " + parent.field("surname"));

			children = r.field("children");

			if (children != null) {
				for (ORecordVObject c : children) {
					parent = (ORecordVObject) c.field("parent");

					Assert.assertEquals(r, parent);

					System.out.println("- child: " + c.field("name") + " " + c.field("surname") + " (parent: " + parent.field("name") + " "
							+ parent.field("surname") + ")");
				}
			}
		}
	}

	@Test(dependsOnMethods = "testQueryMultiCircular")
	public void close() {
		database.close();
	}
}
