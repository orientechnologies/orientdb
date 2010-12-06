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
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "object" })
public class ObjectAnnotationsTest {
	private ODatabaseObjectTx	database;
	private String						url;
	private Account						account;
	private Profile						profile;

	@Parameters(value = "url")
	public ObjectAnnotationsTest(String iURL) {
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
	public void testOrientObjectIdPlusVersionAnnotationsInTx() {
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

	@Test(dependsOnMethods = "testOrientObjectIdPlusVersionAnnotationsInTx")
	public void clean() {
		database.delete(profile);
		database.delete(account);

		database.close();
	}
}
