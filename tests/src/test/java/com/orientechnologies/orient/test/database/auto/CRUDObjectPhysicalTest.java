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

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.test.domain.animal.Animal;
import com.orientechnologies.orient.test.domain.animal.AnimalRace;
import com.orientechnologies.orient.test.domain.animal.AnimalType;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
public class CRUDObjectPhysicalTest {
	protected static final int	TOT_RECORDS	= 1000;
	protected long							startRecordNumber;
	private ODatabaseObjectTx		database;

	@Parameters(value = "url")
	public CRUDObjectPhysicalTest(String iURL) {
		database = new ODatabaseObjectTx(iURL);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test
	public void cleanAll() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Animal");

		// DELETE ALL THE RECORD IN THE CLUSTER
		for (Object obj : database.browseCluster("Animal"))
			database.delete(obj);

		Assert.assertEquals(database.countClusterElements("Animal"), 0);

		database.close();
	}

	@Test(dependsOnMethods = "cleanAll")
	public void create() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Animal");

		Animal animal;

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			animal = database.newInstance(Animal.class);

			animal.setId((int) i);
			animal.setName("Gipsy");
			animal.setType("Cat");
			animal.setRace("European");
			animal.setLocation("Italy");
			animal.setPrice(i + 300);

			database.save(animal);
		}

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClusterElements("Animal") - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void readAndBrowseDescendingAndCheckHoleUtilization() {
		database.open("admin", "admin");

		// BROWSE ALL THE OBJECTS
		int i = 0;
		for (Animal a : database.browseClass(Animal.class)) {

			Assert.assertTrue(a.getId() == i);
			Assert.assertEquals(a.getName(), "Gipsy");
			Assert.assertEquals(a.getType(), "Cat");
			Assert.assertEquals(a.getRace(), "European");
			Assert.assertEquals(a.getLocation(), "Italy");
			Assert.assertTrue(a.getPrice() == i + 300f);

			i++;
		}

		Assert.assertTrue(i == TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
	public void update() {
		database.open("admin", "admin");

		int i = 0;
		Animal a;
		for (Object o : database.browseCluster("Animal")) {
			a = (Animal) o;

			if (i % 2 == 0)
				a.setLocation("Spain");

			a.setPrice(i + 100);

			database.save(a);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "update")
	public void testUpdate() {
		database.open("admin", "admin");

		int i = 0;
		Animal a;
		for (OObjectIteratorCluster<Animal> iterator = database.browseCluster("Animal"); iterator.hasNext();) {
			a = iterator.next();

			if (i % 2 == 0)
				Assert.assertEquals(a.getLocation(), "Spain");
			else
				Assert.assertEquals(a.getLocation(), "Italy");

			Assert.assertEquals(a.getPrice(), i + 100f);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "testUpdate")
	public void delete() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Animal");

		// DELETE ALL THE RECORD IN THE CLUSTER
		for (Object obj : database.browseCluster("Animal"))
			database.delete(obj);

		Assert.assertEquals(database.countClusterElements("Animal"), 0);

		database.close();
	}

	@Test(dependsOnMethods = "delete")
	public void createLinked() {
		database.open("admin", "admin");

		long animalTypes = database.countClass("AnimalType");
		long animalRaces = database.countClass("AnimalRace");

		AnimalType animalType = new AnimalType();
		animalType.setName("Cat");
		animalType.getRaces().add(new AnimalRace("European"));
		animalType.getRaces().add(new AnimalRace("Siamese"));

		database.save(animalType);

		Assert.assertEquals(database.countClass("AnimalType"), animalTypes + 1);
		Assert.assertEquals(database.countClass("AnimalRace"), animalRaces);

		database.close();
	}

	@Test(dependsOnMethods = "createLinked")
	public void queryLinked() {
		database.open("admin", "admin");

		for (AnimalType obj : database.browseClass(AnimalType.class)) {
			Assert.assertEquals(obj.getRaces().size(), 2);
		}

		database.close();
	}
}
