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

import java.util.Date;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "crud", "record-vobject" }, sequential = true)
public class CRUDObjectPhysicalTest {
	protected static final int	TOT_RECORDS	= 1000;
	protected long							startRecordNumber;
	private ODatabaseObjectTx		database;
	private City								rome				= new City(new Country("Italy"), "Rome");

	@Parameters(value = "url")
	public CRUDObjectPhysicalTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());

		database = new ODatabaseObjectTx(iURL);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test
	public void create() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Account");

		Account account;

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			account = new Account((int) i, "Bill", "Gates");
			account.setBirthDate(new Date());
			account.setSalary(i + 300f);
			account.getAddresses().add(new Address("Residence", rome, "Piazza Navona, 1"));
			database.save(account);
		}

		database.close();
	}

	@Test(dependsOnMethods = "create")
	public void testCreate() {
		database.open("admin", "admin");

		Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void readAndBrowseDescendingAndCheckHoleUtilization() {
		database.open("admin", "admin");

		// BROWSE ALL THE OBJECTS
		int i = 0;
		for (Account a : database.browseClass(Account.class)) {

			Assert.assertTrue(a.getId() == i);
			Assert.assertEquals(a.getName(), "Bill");
			Assert.assertEquals(a.getSurname(), "Gates");
			Assert.assertTrue(a.getSalary() == i + 300f);
			Assert.assertEquals(a.getAddresses().size(), 1);
			Assert.assertEquals(a.getAddresses().get(0).getCity().getName(), rome.getName());
			Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), rome.getCountry().getName());

			i++;
		}

		Assert.assertTrue(i == TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
	public void update() {
		database.open("admin", "admin");

		int i = 0;
		Account a;
		for (Object o : database.browseCluster("Account")) {
			a = (Account) o;

			if (i % 2 == 0)
				a.getAddresses().set(0, new Address("work", new City(new Country("Spain"), "Madrid"), "Plaza central"));

			a.setSalary(i + 100);

			database.save(a);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "update")
	public void testUpdate() {
		database.open("admin", "admin");

		int i = 0;
		Account a;
		for (OObjectIteratorCluster<Account> iterator = database.browseCluster("Account"); iterator.hasNext();) {
			a = iterator.next();

			if (i % 2 == 0)
				Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), "Spain");
			else
				Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), "Italy");

			Assert.assertEquals(a.getSalary(), i + 100f);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "testUpdate")
	public void delete() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Account");

		// DELETE ALL THE RECORD IN THE CLUSTER
		for (Object obj : database.browseCluster("Account"))
			database.delete(obj);

		Assert.assertEquals(database.countClusterElements("Account"), 0);

		database.close();
	}

	@Test(dependsOnMethods = "delete")
	public void createLinked() {
		database.open("admin", "admin");

		long profiles = database.countClass("Profile");

		Profile neo = new Profile("Neo");
		neo.addFollowing(new Profile("Morpheus"));
		neo.addFollowing(new Profile("Trinity"));

		database.save(neo);

		Assert.assertEquals(database.countClass("Profile"), profiles + 3);

		database.close();
	}

	@Test(dependsOnMethods = "createLinked")
	public void queryLinked() {
		database.open("admin", "admin");

		for (Profile obj : database.browseClass(Profile.class)) {
			if (obj.getNick().equals("Neo"))
				Assert.assertEquals(obj.getFollowings().size(), 2);
			else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity"))
				Assert.assertEquals(obj.getFollowings().size(), 0);
		}

		database.close();
	}

	@Test(dependsOnMethods = "queryLinked")
	public void cleanAll() {
		database.open("admin", "admin");

		startRecordNumber = database.countClusterElements("Account");

		// DELETE ALL THE RECORD IN THE CLUSTER
		for (Object obj : database.browseCluster("Account"))
			database.delete(obj);

		Assert.assertEquals(database.countClusterElements("Account"), 0);

		database.close();
	}

}
