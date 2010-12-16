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

import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.exception.OTransactionException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "object" })
public class ObjectDetachingTest {
	private ODatabaseObjectTx	database;
	private String						url;
	private Account						account;
	private Profile						profile;

	@Parameters(value = "url")
	public ObjectDetachingTest(String iURL) {
		url = iURL;
	}

	@Test
	public void createAnnotatedObjects() {
		database = new ODatabaseObjectTx(url).open("admin", "admin");

		Country austria = new Country("Austria");
		City graz = new City(austria, "Graz");
		database.save(graz);

		account = new Account();
		database.save(account);

		profile = new Profile();
		database.save(profile);
	}

	@Test(dependsOnMethods = "createAnnotatedObjects")
	public void testJPALongIdAnnotation() {
		// BROWSE ALL THE OBJECTS
		Assert.assertTrue(database.countClass(City.class) > 0);
		for (City c : database.browseClass(City.class)) {
			Assert.assertNotNull(c.getId());
		}
	}

	@Test(dependsOnMethods = "testJPALongIdAnnotation")
	public void testJPAObjectIdAnnotation() {
		// BROWSE ALL THE OBJECTS
		Assert.assertTrue(database.countClass(Account.class) > 0);
		for (Account a : database.browseClass(Account.class)) {
			Assert.assertNotNull(a.getRid());
		}
	}

	@Test(dependsOnMethods = "testJPAObjectIdAnnotation")
	public void testOrientObjectIdAnnotation() {
		// BROWSE ALL THE OBJECTS
		Assert.assertTrue(database.countClass(Country.class) > 0);
		for (Country c : database.browseClass(Country.class)) {
			Assert.assertNotNull(c.getId());
		}
	}

	@Test(dependsOnMethods = "testOrientObjectIdAnnotation")
	public void testOrientStringIdAnnotation() {
		// BROWSE ALL THE OBJECTS
		Assert.assertTrue(database.countClass(Profile.class) > 0);
		for (Profile a : database.browseClass(Profile.class)) {
			Assert.assertNotNull(a.getId());
		}
	}

	@SuppressWarnings("unchecked")
	@Test(dependsOnMethods = "testOrientStringIdAnnotation")
	public void testOrientObjectIdPlusVersionAnnotationsNotInTx() {
		// BROWSE ALL THE OBJECTS
		Assert.assertTrue(database.countClass(Country.class) > 0);
		for (Country c : (List<Country>) database.query(new OSQLSynchQuery<Object>("select from Country where name = 'Austria'"))) {
			Assert.assertNotNull(c.getId());
			Assert.assertNotNull(c.getVersion());

			// UPDATE IT TO GET NEWER VERSION
			c.setName(c.getName() + " v1");
			database.save(c);

			// CHECK VERSION
			Assert.assertTrue((Integer) c.getVersion() > 0);
		}

		// BROWSE ALL THE OBJECTS
		for (Country c : (List<Country>) database.query(new OSQLSynchQuery<Object>("select from Country where name = 'Austria'"))) {
			Assert.assertNotNull(c.getId());
			Assert.assertNotNull(c.getVersion());
			Assert.assertTrue((Integer) c.getVersion() > 0);
		}
	}

	@Test(dependsOnMethods = "testOrientObjectIdPlusVersionAnnotationsNotInTx", expectedExceptions = OTransactionException.class)
	public void testOrientObjectIdPlusVersionAnnotationsInTx() {
		database.begin();

		try {
			// BROWSE ALL THE OBJECTS
			Assert.assertTrue(database.countClass(Account.class) > 0);
			for (Account a : database.browseClass(Account.class)) {
				Assert.assertNotNull(a.getId());

				// UPDATE IT TO GET NEWER VERSION
				a.setName(a.getName() + " v1");
				database.save(a);
				break;
			}

			database.commit();

			Assert.assertTrue(false);
		} finally {
			database.rollback();
		}
	}

	@Test(dependsOnMethods = "testOrientObjectIdPlusVersionAnnotationsInTx")
	public void testInsertCommit() {
		String initialCountryName = "insertCommit";
		Country country = new Country(initialCountryName);

		long initCount = database.countClass(Country.class);

		database.begin();
		database.save(country);
		database.commit();

		Assert.assertEquals(database.countClass(Country.class), initCount + 1);
		Assert.assertNotNull(country.getId());
		Assert.assertNotNull(country.getVersion());

		Country found = (Country) database.load((ORecordId) country.getId());
		Assert.assertNotNull(found);
		Assert.assertEquals(country.getName(), found.getName());
	}

	@Test(dependsOnMethods = "testInsertCommit")
	public void testInsertRollback() {
		String initialCountryName = "insertRollback";
		Country country = new Country(initialCountryName);

		long initCount = database.countClass(Country.class);

		database.begin();
		database.save(country);
		database.rollback();

		Assert.assertEquals(database.countClass(Country.class), initCount);
		Assert.assertNull(country.getId());
		Assert.assertNull(country.getVersion());
	}

	@Test(dependsOnMethods = "testInsertRollback")
	public void testUpdateCommit() {
		String initialCountryName = "updateCommit";
		Country country = new Country(initialCountryName);

		database.save(country);
		Assert.assertNotNull(country.getId());
		Assert.assertNotNull(country.getVersion());

		Integer initVersion = (Integer) country.getVersion();

		database.begin();
		Country loaded = (Country) database.load((ORecordId) country.getId());
		Assert.assertEquals(loaded, country);
		String newName = "ShouldBeChanged";
		loaded.setName(newName);
		database.save(loaded);
		database.commit();

		loaded = (Country) database.load((ORecordId) country.getId());
		Assert.assertTrue(loaded.equals(country));
		Assert.assertEquals(loaded.getId(), country.getId());
		Assert.assertEquals(loaded.getVersion(), new Integer(initVersion + 1));
		Assert.assertEquals(loaded.getName(), newName);
	}

	@Test(dependsOnMethods = "testUpdateCommit")
	public void testUpdateRollback() {
		String initialCountryName = "updateRollback";
		Country country = new Country(initialCountryName);

		database.save(country);
		Assert.assertNotNull(country.getId());
		Assert.assertNotNull(country.getVersion());

		Integer initVersion = (Integer) country.getVersion();

		database.begin();
		Country loaded = (Country) database.load((ORecordId) country.getId());
		Assert.assertEquals(loaded, country);
		String newName = "ShouldNotBeChanged";
		loaded.setName(newName);
		database.save(loaded);
		database.rollback();

		loaded = (Country) database.load((ORecordId) country.getId());
		Assert.assertNotSame(loaded, country);
		Assert.assertEquals(loaded.getVersion(), initVersion);
		Assert.assertEquals(loaded.getName(), initialCountryName);
	}

	@Test(dependsOnMethods = "testUpdateRollback")
	public void testDeleteCommit() {
		String initialCountryName = "deleteCommit";
		Country Country = new Country(initialCountryName);

		long initCount = database.countClass(Country.class);

		database.save(Country);

		Assert.assertEquals(database.countClass(Country.class), initCount + 1);

		database.begin();
		database.delete(Country);
		database.commit();

		Assert.assertEquals(database.countClass(Country.class), initCount);
		Country found = (Country) database.load((ORecordId) Country.getId());
		Assert.assertNull(found);
	}

	@Test(dependsOnMethods = "testDeleteCommit")
	public void testDeleteRollback() {
		String initialCountryName = "deleteRollback";
		Country country = new Country(initialCountryName);

		long initCount = database.countClass(Country.class);

		database.save(country);

		Assert.assertEquals(database.countClass(Country.class), initCount + 1);

		database.begin();
		database.delete(country);
		database.rollback();

		Assert.assertEquals(database.countClass(Country.class), initCount + 1);
		Country found = (Country) database.load((ORecordId) country.getId());
		Assert.assertNotNull(found);
		Assert.assertEquals(found.getName(), country.getName());
	}

	@Test(dependsOnMethods = "testDeleteRollback")
	public void clean() {
		database.close();

		database = new ODatabaseObjectTx(url).open("admin", "admin");

		database.delete(profile);
		database.delete(account);

		database.close();
	}
}
