/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
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
import java.util.Iterator;
import java.util.List;

import javassist.util.proxy.Proxy;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.OLazyObjectSetInterface;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.ODatabaseObjectTx;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.iterator.OObjectIteratorClass;
import com.orientechnologies.orient.object.iterator.OObjectIteratorCluster;
import com.orientechnologies.orient.test.domain.base.EmbeddedChild;
import com.orientechnologies.orient.test.domain.base.EnumTest;
import com.orientechnologies.orient.test.domain.base.IdObject;
import com.orientechnologies.orient.test.domain.base.Instrument;
import com.orientechnologies.orient.test.domain.base.JavaComplexTestClass;
import com.orientechnologies.orient.test.domain.base.JavaSimpleTestClass;
import com.orientechnologies.orient.test.domain.base.JavaTestInterface;
import com.orientechnologies.orient.test.domain.base.Musician;
import com.orientechnologies.orient.test.domain.base.Parent;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.Child;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "crud", "object" })
public class CRUDObjectPhysicalTest {
	protected static final int	TOT_RECORDS	= 100;
	protected long							startRecordNumber;
	private OObjectDatabaseTx		database;
	private City								rome				= new City(new Country("Italy"), "Rome");
	private String							url;

	@Parameters(value = "url")
	public CRUDObjectPhysicalTest(String iURL) {
		url = iURL;
	}

	@Test
	public void create() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
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

	@Test(dependsOnMethods = "create", expectedExceptions = UnsupportedOperationException.class)
	public void testReleasedPoolDatabase() {
		database.open("admin", "admin");
	}

	@Test(dependsOnMethods = "testReleasedPoolDatabase")
	public void testCreate() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		Assert.assertEquals(database.countClusterElements("Account") - startRecordNumber, TOT_RECORDS);

		database.close();
	}

	@Test(dependsOnMethods = "testCreate")
	public void testAutoCreateClass() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		Assert.assertNull(database.getMetadata().getSchema().getClass(Dummy.class.getSimpleName()));

		database.getEntityManager().registerEntityClass(Dummy.class);

		database.countClass(Dummy.class.getSimpleName());

		Assert.assertNotNull(database.getMetadata().getSchema().getClass(Dummy.class.getSimpleName()));

		database.close();
	}

	public void testSimpleTypes() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		JavaSimpleTestClass javaObj = database.newInstance(JavaSimpleTestClass.class);
		Assert.assertEquals(javaObj.getText(), "initTest");
		javaObj.setText("test");
		javaObj.setNumberSimple(12345);
		javaObj.setDoubleSimple(12.34d);
		javaObj.setFloatSimple(123.45f);
		javaObj.setLongSimple(12345678l);
		javaObj.setByteSimple((byte) 1);
		javaObj.setFlagSimple(true);
		javaObj.setEnumeration(EnumTest.ENUM1);

		JavaSimpleTestClass savedJavaObj = database.save(javaObj);
		ORecordId id = (ORecordId) savedJavaObj.getId();
		database.close();

		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		JavaSimpleTestClass loadedJavaObj = (JavaSimpleTestClass) database.load(id);
		Assert.assertEquals(loadedJavaObj.getText(), "test");
		Assert.assertEquals(loadedJavaObj.getNumberSimple(), 12345);
		Assert.assertEquals(loadedJavaObj.getDoubleSimple(), 12.34d);
		Assert.assertEquals(loadedJavaObj.getFloatSimple(), 123.45f);
		Assert.assertEquals(loadedJavaObj.getLongSimple(), 12345678l);
		Assert.assertEquals(loadedJavaObj.getByteSimple(), (byte) 1);
		Assert.assertEquals(loadedJavaObj.getFlagSimple(), true);
		Assert.assertEquals(loadedJavaObj.getEnumeration(), EnumTest.ENUM1);
		Assert.assertTrue(loadedJavaObj.getTestAnonymous() instanceof JavaTestInterface);
		Assert.assertEquals(loadedJavaObj.getTestAnonymous().getNumber(), -1);
		loadedJavaObj.setEnumeration(EnumTest.ENUM2);
		loadedJavaObj.setTestAnonymous(new JavaTestInterface() {

			public int getNumber() {
				return 0;
			}
		});
		database.save(loadedJavaObj);

		database.close();

		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		loadedJavaObj = (JavaSimpleTestClass) database.load(id);
		Assert.assertEquals(loadedJavaObj.getEnumeration(), EnumTest.ENUM2);
		Assert.assertTrue(loadedJavaObj.getTestAnonymous() instanceof JavaTestInterface);
		Assert.assertEquals(loadedJavaObj.getTestAnonymous().getNumber(), -1);
	}

	@Test(dependsOnMethods = "testAutoCreateClass")
	public void readAndBrowseDescendingAndCheckHoleUtilization() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getLevel1Cache().invalidate();
		database.getLevel2Cache().clear();

		// BROWSE ALL THE OBJECTS
		int i = 0;
		for (Account a : database.browseClass(Account.class)) {

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

	@Test(dependsOnMethods = "testAutoCreateClass")
	public void synchQueryCollectionsFetch() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getLevel1Cache().invalidate();
		database.getLevel2Cache().clear();

		// BROWSE ALL THE OBJECTS
		int i = 0;
		List<Account> result = database.query(new OSQLSynchQuery<Account>("select from Account").setFetchPlan("*:-1"));
		for (Account a : result) {

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

	@Test(dependsOnMethods = "testAutoCreateClass")
	public void synchQueryCollectionsFetchNoLazyLoad() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getLevel1Cache().invalidate();
		database.getLevel2Cache().clear();
		database.setLazyLoading(false);

		// BROWSE ALL THE OBJECTS
		int i = 0;
		List<Account> result = database.query(new OSQLSynchQuery<Account>("select from Account").setFetchPlan("*:2"));
		for (Account a : result) {

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		// BROWSE ALL THE OBJECTS
		for (OUser u : database.browseClass(OUser.class)) {
			u.save();
		}

		database.close();
	}

	@Test(dependsOnMethods = "mapEnumAndInternalObjects")
	public void mapObjectsTest() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		JavaComplexTestClass p = database.newInstance(JavaComplexTestClass.class);
		p.setName("Silvester");

		Child c = database.newInstance(Child.class);
		c.setName("John");

		p.getChildren().put("first", c);

		p.getEnumList().add(EnumTest.ENUM1);
		p.getEnumList().add(EnumTest.ENUM2);

		p.getEnumSet().add(EnumTest.ENUM1);
		p.getEnumSet().add(EnumTest.ENUM3);

		p.getEnumMap().put("1", EnumTest.ENUM2);
		p.getEnumMap().put("2", EnumTest.ENUM3);

		database.save(p);

		List<Child> cresult = database.query(new OSQLSynchQuery<Child>("select * from Child"));

		Assert.assertTrue(cresult.size() > 0);

		ORID rid = database.getRecordByUserObject(p, false).getIdentity();

		database.close();

		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		JavaComplexTestClass loaded = database.load(rid);

		Assert.assertEquals(loaded.getEnumList().size(), 2);
		Assert.assertEquals(loaded.getEnumList().get(0), EnumTest.ENUM1);
		Assert.assertEquals(loaded.getEnumList().get(1), EnumTest.ENUM2);

		Assert.assertEquals(loaded.getEnumSet().size(), 2);
		Iterator<EnumTest> it = loaded.getEnumSet().iterator();
		Assert.assertEquals(it.next(), EnumTest.ENUM1);
		Assert.assertEquals(it.next(), EnumTest.ENUM3);

		Assert.assertEquals(loaded.getEnumMap().size(), 2);
		Assert.assertEquals(loaded.getEnumMap().get("1"), EnumTest.ENUM2);
		Assert.assertEquals(loaded.getEnumMap().get("2"), EnumTest.ENUM3);
	}

	@Test(dependsOnMethods = "mapEnumAndInternalObjects")
	public void afterDeserializationCall() {
		// COMMENTED SINCE SERIALIZATION AND DESERIALIZATION

		// database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		// // TODO TO DELETE WHEN IMPLEMENTED STATIC ENTITY MANAGER
		// database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
		//
		// // BROWSE ALL THE OBJECTS
		// for (Account a : database.browseClass(Account.class)) {
		// Assert.assertTrue(a.isInitialized());
		// }
		// database.close();
	}

	@Test(dependsOnMethods = "afterDeserializationCall")
	public void update() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		int i = 0;
		Account a;
		for (Object o : database.browseCluster("Account").setFetchPlan("*:1")) {
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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		int i = 0;
		Account a;
		for (OObjectIteratorCluster<Account> iterator = database.browseCluster("Account"); iterator.hasNext();) {
			iterator.setFetchPlan("*:1");
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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		for (Profile obj : database.browseClass(Profile.class).setFetchPlan("*:1")) {
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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.setLazyLoading(false);
		for (Profile obj : database.browseClass(Profile.class).setFetchPlan("*:1")) {
			Assert.assertTrue(!(obj.getFollowings() instanceof OLazyObjectSetInterface)
					|| ((OLazyObjectSetInterface<Profile>) obj.getFollowings()).isConverted());
			Assert.assertTrue(!(obj.getFollowers() instanceof OLazyObjectSetInterface)
					|| ((OLazyObjectSetInterface<Profile>) obj.getFollowers()).isConverted());
			if (obj.getNick().equals("Neo")) {
				Assert.assertEquals(obj.getFollowers().size(), 0);
				Assert.assertEquals(obj.getFollowings().size(), 2);
			} else if (obj.getNick().equals("Morpheus") || obj.getNick().equals("Trinity")) {
				Assert.assertEquals(obj.getFollowings().size(), 0);
				Assert.assertEquals(obj.getFollowers().size(), 1);
				Assert.assertTrue(obj.getFollowers().iterator().next() instanceof Profile);
			}
		}

		database.close();
	}

	@Test(dependsOnMethods = "checkLazyLoadingOff")
	public void queryPerFloat() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
		List<Profile> result = database.command(query).execute("Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithPositionalParameters() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getMetadata().getSchema().reload();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where name = ? and surname = ?");
		List<Profile> result = database.query(query, "Barack", "Obama");

		Assert.assertTrue(result.size() != 0);

		database.close();
	}

	@Test
	public void queryWithRidAsParameters() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getMetadata().getSchema().reload();

		Profile profile = (Profile) database.browseClass("Profile").next();

		final OSQLSynchQuery<Profile> query = new OSQLSynchQuery<Profile>("select from Profile where @rid = ?");
		List<Profile> result = database.query(query, new ORecordId(profile.getId()));

		Assert.assertEquals(result.size(), 1);

		database.close();
	}

	@Test
	public void queryWithRidStringAsParameters() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getMetadata().getSchema().reload();

		Assert.assertTrue(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country'")).size() > 0);
		Assert.assertEquals(database.query(new OSQLSynchQuery<Profile>("select from City where country.@class = 'Country22'")).size(),
				0);

		database.close();
	}

	@Test
	public void queryPreparredTwice() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

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

	@SuppressWarnings("deprecation")
	public void testOldObjectImplementation() {
		ODatabaseObjectTx db = new ODatabaseObjectTx(url).open("admin", "admin");
		db.getEntityManager().registerEntityClasses("com.e_soa.dbobjects");
		// insert some instruments
		Instrument instr = new Instrument("Fender Stratocaster");
		db.save(instr);
		Instrument instr2 = new Instrument("Music Man");
		db.save(instr2);
		// Insert some musicians
		Musician man = new Musician();
		man.setName("Jack");
		OObjectIteratorClass<Object> list = db.browseClass("Instrument");
		for (Object anInstrument : list) {
			man.getInstruments().add((Instrument) anInstrument);
		}
		db.save(man);
		Musician man2 = new Musician();
		man2.setName("Roger");
		String query = "select from Instrument where name like 'Fender%'";
		List<IdObject> list2 = db.query(new OSQLSynchQuery<ODocument>(query));
		Assert.assertTrue(!(list2.get(0) instanceof Proxy));
		man2.getInstruments().add((Instrument) list2.get(0));
		db.save(man2);
		//
		db.close();
		db = new ODatabaseObjectTx(url).open("admin", "admin");
		db.getEntityManager().registerEntityClasses("com.e_soa.dbobjects");
		query = "select from Musician limit 1";
		List<IdObject> list3 = db.query(new OSQLSynchQuery<ODocument>(query));
		man = (Musician) list3.get(0);
		Assert.assertTrue(!(man instanceof Proxy));
		for (Object aObject : man.getInstruments()) {
			Assert.assertTrue(!(aObject instanceof Proxy));
		}
		db.close();
		db = new ODatabaseObjectTx(url).open("admin", "admin");
		list3 = db.query(new OSQLSynchQuery<ODocument>(query));
		man = (Musician) list3.get(0);
		man.setName("Big Jack");
		db.save(man); // here is the exception
		db.close();
	}

	public void testEmbeddedDeletion() throws Exception {
		OObjectDatabaseTx db = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		Parent parent = db.newInstance(Parent.class);
		parent.setName("Big Parent");

		EmbeddedChild embedded = db.newInstance(EmbeddedChild.class);
		embedded.setName("Little Child");

		parent.setEmbeddedChild(embedded);

		parent = db.save(parent);

		List<Parent> presult = db.query(new OSQLSynchQuery<Parent>("select from Parent"));
		List<EmbeddedChild> cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select from EmbeddedChild"));
		Assert.assertEquals(presult.size(), 1);
		Assert.assertEquals(cresult.size(), 0);

		EmbeddedChild child = db.newInstance(EmbeddedChild.class);
		child.setName("Little Child");
		parent.setChild(child);

		parent = db.save(parent);

		presult = db.query(new OSQLSynchQuery<Parent>("select from Parent"));
		cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select from EmbeddedChild"));
		Assert.assertEquals(presult.size(), 1);
		Assert.assertEquals(cresult.size(), 1);

		db.delete(parent);

		presult = db.query(new OSQLSynchQuery<Parent>("select * from Parent"));
		cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select * from EmbeddedChild"));

		Assert.assertEquals(presult.size(), 0);
		Assert.assertEquals(cresult.size(), 1);

		db.delete(child);

		presult = db.query(new OSQLSynchQuery<Parent>("select * from Parent"));
		cresult = db.query(new OSQLSynchQuery<EmbeddedChild>("select * from EmbeddedChild"));

		Assert.assertEquals(presult.size(), 0);
		Assert.assertEquals(cresult.size(), 0);

		db.close();
	}

	public void testEmbeddedBinary() {
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

		database.getMetadata().getSchema().reload();

		Account a = new Account(0, "Chris", "Martin");
		a.setThumbnail(new byte[] { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 });
		a = database.save(a);
		database.close();

		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		Account aa = (Account) database.load((ORID) a.getRid());
		Assert.assertNotNull(a.getThumbnail());
		Assert.assertNotNull(aa.getThumbnail());
		byte[] b = aa.getThumbnail();
		for (int i = 0; i < 10; ++i)
			Assert.assertEquals(b[i], i);

		// TO REFACTOR OR DELETE SINCE SERIALIZATION AND DESERIALIZATION DON'T APPLY ANYMORE
		// Assert.assertNotNull(aa.getPhoto());
		// b = aa.getPhoto();
		// for (int i = 0; i < 10; ++i)
		// Assert.assertEquals(b[i], i);

		database.close();
	}
}
