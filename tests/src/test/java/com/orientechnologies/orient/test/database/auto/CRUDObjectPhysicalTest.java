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
import java.util.HashMap;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectPool;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.object.OLazyObjectSet;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "crud", "object" })
public class CRUDObjectPhysicalTest {
	protected static final int	TOT_RECORDS	= 100;
	protected long							startRecordNumber;
	private ODatabaseObjectTx		database;
	private City								rome				= new City(new Country("Italy"), "Rome");
	private String							url;

	@Parameters(value = "url")
	public CRUDObjectPhysicalTest(String iURL) {
		url = iURL;
	}

	@Test
	public void create() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");

		startRecordNumber = database.countClusterElements("Account");

		Account account;

		for (long i = startRecordNumber; i < startRecordNumber + TOT_RECORDS; ++i) {
			account = new Account((int) i, "Bill", "Gates");
			account.setBirthDate(new Date());
			account.setSalary(i + 300.10f);
			account.getAddresses().add(new Address("Residence", rome, "Piazza Navona, 1"));
			database.save(account);
		}

		database.close();
	}

	@Test(dependsOnMethods = "create", expectedExceptions = ODatabaseException.class)
	public void testReleasedPoolDatabase() {
		database.open("admin", "admin");
	}

	@Test(dependsOnMethods = "testReleasedPoolDatabase")
	public void testCreate() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void testAutoCreateClass() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		Assert.assertNull(database.getMetadata().getSchema().getClass(Dummy.class.getSimpleName()));

		database.getEntityManager().registerEntityClass(Dummy.class);

		database.countClass(Dummy.class.getSimpleName());

		Assert.assertNotNull(database.getMetadata().getSchema().getClass(Dummy.class.getSimpleName()));

		database.close();
	}

	@Test(dependsOnMethods = "testAutoCreateClass")
	public void readAndBrowseDescendingAndCheckHoleUtilization() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		database.getLevel1Cache().invalidate();
		database.getLevel2Cache().clear();

		// BROWSE ALL THE OBJECTS
		int i = 0;
		for (Account a : database.browseClass(Account.class).setFetchPlan("*:-1")) {

			Assert.assertEquals(a.getId(), i);
			Assert.assertEquals(a.getName(), "Bill");
			Assert.assertEquals(a.getSurname(), "Gates");
			Assert.assertEquals(a.getSalary(), i + 300.1f);
			Assert.assertEquals(a.getAddresses().size(), 1);
			Assert.assertEquals(a.getAddresses().get(0).getCity().getName(), rome.getName());
			Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), rome.getCountry().getName());

			i++;
		}

		Assert.assertTrue(i == TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "readAndBrowseDescendingAndCheckHoleUtilization")
	public void mapEnumAndInternalObjects() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		// BROWSE ALL THE OBJECTS
		for (OUser u : database.browseClass(OUser.class)) {
			u.save();
		}

		database.close();
	}

	@Test(dependsOnMethods = "mapEnumAndInternalObjects")
	public void afterDeserializationCall() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		// BROWSE ALL THE OBJECTS
		for (Account a : database.browseClass(Account.class)) {
			Assert.assertTrue(a.isInitialized());
		}

		database.close();
	}

	@Test(dependsOnMethods = "afterDeserializationCall")
	public void update() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		int i = 0;
		Account a;
		for (Object o : database.browseCluster("Account")) {
			a = (Account) o;

			if (i % 2 == 0)
				a.getAddresses().set(0, new Address("work", new City(new Country("Spain"), "Madrid"), "Plaza central"));

			a.setSalary(i + 500.10f);

			database.save(a);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "update")
	public void testUpdate() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		int i = 0;
		Account a;
		for (OObjectIteratorCluster<Account> iterator = database.browseCluster("Account"); iterator.hasNext();) {
			a = iterator.next();

			if (i % 2 == 0)
				Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), "Spain");
			else
				Assert.assertEquals(a.getAddresses().get(0).getCity().getCountry().getName(), "Italy");

			Assert.assertEquals(a.getSalary(), i + 500.1f);

			i++;
		}

		database.close();
	}

	@Test(dependsOnMethods = "testUpdate")
	public void createLinked() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		long profiles = database.countClass("Profile");

		Profile neo = new Profile("Neo").setValue("test").setLocation(
				new Address("residence", new City(new Country("Spain"), "Madrid"), "Rio de Castilla"));
		neo.addFollowing(new Profile("Morpheus"));
		neo.addFollowing(new Profile("Trinity"));

		database.save(neo);

		Assert.assertEquals(database.countClass("Profile"), profiles + 3);

		database.close();
	}

	@Test(dependsOnMethods = "createLinked")
	public void browseLinked() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		for (Profile obj : database.browseClass(Profile.class)) {
			if (obj.getNick().equals("Neo")) {
				Assert.assertEquals(obj.getFollowers().size(), 0);
				Assert.assertEquals(obj.getFollowings().size(), 2);
			} else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity")) {
				Assert.assertEquals(obj.getFollowers().size(), 1);
				Assert.assertEquals(obj.getFollowings().size(), 0);
			}
		}

		database.close();
	}

	@Test(dependsOnMethods = "createLinked")
	public void checkLazyLoadingOff() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		database.setLazyLoading(false);
		for (Profile obj : database.browseClass(Profile.class)) {
			Assert.assertFalse(obj.getFollowings() instanceof OLazyObjectSet);
			Assert.assertFalse(obj.getFollowers() instanceof OLazyObjectSet);
			if (obj.getNick().equals("Neo")) {
				Assert.assertEquals(obj.getFollowers().size(), 0);
				Assert.assertEquals(obj.getFollowings().size(), 2);
			} else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity")) {
				Assert.assertEquals(obj.getFollowers().size(), 1);
				Assert.assertEquals(obj.getFollowings().size(), 0);
			}
		}

		database.close();
	}

	@Test(dependsOnMethods = "checkLazyLoadingOff")
	public void queryPerFloat() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");

		final List<Account> result = database.query(new OSQLSynchQuery<ODocument>("select * from Account where salary = 500.10"));

		Assert.assertTrue(result.size() > 0);

		Account account;
		for (int i = 0; i < result.size(); ++i) {
			account = result.get(i);

			Assert.assertEquals(account.getSalary(), 500.10f);
		}

		database.close();
	}

	@Test(dependsOnMethods = "queryPerFloat")
	public void queryCross3Levels() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final List<Profile> result = database.query(new OSQLSynchQuery<Profile>(
				"select from Profile where location.city.country.name = 'Spain'"));

		Assert.assertTrue(result.size() > 0);

		Profile profile;
		for (int i = 0; i < result.size(); ++i) {
			profile = result.get(i);

			Assert.assertEquals(profile.getLocation().getCity().getCountry().getName(), "Spain");
		}

		database.close();
	}

	@Test(dependsOnMethods = "queryCross3Levels")
	public void deleteFirst() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		startRecordNumber = database.countClusterElements("Account");

		// DELETE ALL THE RECORD IN THE CLUSTER
		for (Object obj : database.browseCluster("Account")) {
			database.delete(obj);
			break;
		}

		Assert.assertEquals(database.countClusterElements("Account"), startRecordNumber - 1);

		database.close();
	}

	@Test
	public void commandWithPositionalParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
		List<Profile> result = database.command(query).execute("Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithPositionalParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
		List<Profile> result = database.query(query, "Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithRidAsParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		Profile profile = (Profile) database.browseClass("Profile").next();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
		List<Profile> result = database.query(query, new ORecordId(profile.getId()));

		Assert.assertEquals(result.size(), 1);

		database.close();
	}

	@Test
	public void queryWithRidStringAsParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		Profile profile = (Profile) database.browseClass("Profile").next();

		OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
		List<Profile> result = database.query(query, profile.getId());

		Assert.assertEquals(result.size(), 1);

		// TEST WITHOUT # AS PREFIX
		query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
		result = database.query(query, profile.getId().substring(1));

		Assert.assertEquals(result.size(), 1);

		database.close();
	}

	@Test
	public void commandWithNamedParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<Profile> result = database.command(query).execute(params);
		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test(expectedExceptions = OQueryParsingException.class)
	public void commandWithWrongNamedParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
				"select from Profile where name = :name and surname = :surname%");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<Profile> result = database.command(query).execute(params);
		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithNamedParameters() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<Profile> result = database.query(query, params);
		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithObjectAsParameter() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<Profile> result = database.query(query, params);
		Assert.assertTrue(result.size() != 0);

		Profile obama = result.get(0);

		result = database.query(new OSQLSynchQuery<Profile>("select from Profile where followings contains ( @Rid = :who )"), obama);
		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryConcatAttrib() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		Assert.assertTrue(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country'")).size() > 0);
		Assert.assertEquals(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country22'")).size(),
				0);

		database.close();
	}

	@Test
	public void queryPreparredTwice() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<Profile> result = database.query(query, params);
		Assert.assertTrue(result.size() != 0);

		result = database.query(query, params);
		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void commandPreparredTwice() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>(
				"select from Profile where name = :name and surname = :surname");

		HashMap<String, String> params = new HashMap<String, String>();
		params.put("name", "Barack");
		params.put("surname", "Obama");

		List<Profile> result = database.command(query).execute(params);
		Assert.assertTrue(result.size() != 0);

		result = database.command(query).execute(params);
		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	public void testEmbeddedBinary() {
		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		database.getMetadata().getSchema().reload();

		Account a = new Account(0, "Chris", "Martin");
		a.setThumbnail(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
		database.save(a);
		database.close();

		database = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
		Account aa = (Account) database.load((ORID) a.getRid());
		Assert.assertNotNull(a.getThumbnail());
		byte[] b = aa.getThumbnail();
		for (int i = 0; i < 10; ++i)
			Assert.assertEquals(b[i], i);

		Assert.assertNotNull(aa.getPhoto());
		b = aa.getPhoto();
		for (int i = 0; i < 10; ++i)
			Assert.assertEquals(b[i], i);

		database.close();
	}
}
