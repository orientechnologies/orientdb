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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javassist.util.proxy.Proxy;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializer;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabasePool;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.object.enhancement.OObjectEntitySerializer;
import com.orientechnologies.orient.object.serialization.OObjectSerializerContext;
import com.orientechnologies.orient.object.serialization.OObjectSerializerHelper;
import com.orientechnologies.orient.test.domain.base.JavaCascadeDeleteTestClass;
import com.orientechnologies.orient.test.domain.base.JavaSimpleTestClass;
import com.orientechnologies.orient.test.domain.base.Planet;
import com.orientechnologies.orient.test.domain.base.Satellite;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.Child;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.customserialization.Sec;
import com.orientechnologies.orient.test.domain.customserialization.SecurityRole;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "record-object" })
public class ObjectTreeTest {
	private OObjectDatabaseTx	database;
	protected long						startRecordNumber;
	private long							beginCities;
	private String						url;
	protected int							serialized;
	protected int							unserialized;

	public class CustomClass {
		private String								name;
		private Long									age;
		private CustomType						custom;
		private List<CustomType>			customTypeList;
		private Set<CustomType>				customTypeSet;
		private Map<Long, CustomType>	customTypeMap;

		public CustomClass() {
		}

		public CustomClass(String iName, Long iAge, CustomType iCustom, List<CustomType> iCustomTypeList,
				Set<CustomType> iCustomTypeSet, Map<Long, CustomType> iCustomTypeMap) {
			name = iName;
			age = iAge;
			custom = iCustom;
			customTypeList = iCustomTypeList;
			customTypeSet = iCustomTypeSet;
			customTypeMap = iCustomTypeMap;
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public Long getAge() {
			return age;
		}

		public void setAge(Long age) {
			this.age = age;
		}

		public CustomType getCustom() {
			return custom;
		}

		public void setCustom(CustomType custom) {
			this.custom = custom;
		}

		public List<CustomType> getCustomTypeList() {
			return customTypeList;
		}

		public void setCustomTypeList(List<CustomType> customTypeList) {
			this.customTypeList = customTypeList;
		}

		public Set<CustomType> getCustomTypeSet() {
			return customTypeSet;
		}

		public void setCustomTypeSet(Set<CustomType> customTypeSet) {
			this.customTypeSet = customTypeSet;
		}

		public Map<Long, CustomType> getCustomTypeMap() {
			return customTypeMap;
		}

		public void setCustomTypeMap(Map<Long, CustomType> customTypeMap) {
			this.customTypeMap = customTypeMap;
		}
	}

	public class CustomType {
		public long	value;

		public CustomType() {
		}

		public CustomType(Long iFieldValue) {
			value = iFieldValue;
		}

		public long getValue() {
			return value;
		}

		public void setValue(long value) {
			this.value = value;
		}
	}

	public ObjectTreeTest() {
	}

	@Parameters(value = "url")
	public ObjectTreeTest(@Optional(value = "memory:test") String iURL) {
		url = iURL;
	}

	@AfterClass
	public void close() {
		database.close();
	}

	@BeforeClass
	public void open() {
		database = new OObjectDatabaseTx(url);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
		if ("memory:test".equals(database.getURL())) {
			database.create();
		} else {
			database.open("admin", "admin");
		}
	}

	@Test
	public void testPool() throws IOException {
		final OObjectDatabaseTx[] dbs = new OObjectDatabaseTx[OObjectDatabasePool.global().getMaxSize()];

		for (int i = 0; i < 10; ++i) {
			for (int db = 0; db < dbs.length; ++db)
				dbs[db] = OObjectDatabasePool.global().acquire(url, "admin", "admin");
			for (int db = 0; db < dbs.length; ++db)
				dbs[db].close();
		}
	}

	@Test
	public void testPersonSaving() {
		final long beginProfiles = database.countClusterElements("Profile");
		beginCities = database.countClusterElements("City");

		Country italy = database.newInstance(Country.class, "Italy");

		Profile garibaldi = database.newInstance(Profile.class, "GGaribaldi", "Giuseppe", "Garibaldi", null);
		garibaldi.setLocation(database.newInstance(Address.class, "Residence", database.newInstance(City.class, italy, "Rome"),
				"Piazza Navona, 1"));

		Profile bonaparte = database.newInstance(Profile.class, "NBonaparte", "Napoleone", "Bonaparte", garibaldi);
		bonaparte.setLocation(database.newInstance(Address.class, "Residence", garibaldi.getLocation().getCity(),
				"Piazza di Spagna, 111"));
		database.save(bonaparte);

		Assert.assertEquals(database.countClusterElements("Profile"), beginProfiles + 2);
	}

	@Test(dependsOnMethods = "testPersonSaving")
	public void testCitySaving() {
		Assert.assertEquals(database.countClusterElements("City"), beginCities + 1);
	}

	@Test(dependsOnMethods = "testCitySaving")
	public void testCityEquality() {
		List<Profile> resultset = database.query(new OSQLSynchQuery<Object>("select from profile where location.city.name = 'Rome'"));
		Assert.assertEquals(resultset.size(), 2);

		Profile p1 = resultset.get(0);
		Profile p2 = resultset.get(1);

		Assert.assertNotSame(p1, p2);
		Assert.assertSame(OObjectEntitySerializer.getDocument((Proxy) p1.getLocation().getCity()),
				OObjectEntitySerializer.getDocument((Proxy) p2.getLocation().getCity()));
	}

	@Test(dependsOnMethods = "testCityEquality")
	public void testSaveCircularLink() {
		Profile winston = database.newInstance(Profile.class, "WChurcill", "Winston", "Churcill", null);
		winston.setLocation(database.newInstance(Address.class, "Residence",
				database.newInstance(City.class, database.newInstance(Country.class, "England"), "London"), "unknown"));

		Profile nicholas = database.newInstance(Profile.class, "NChurcill", "Nicholas ", "Churcill", winston);
		nicholas.setLocation(winston.getLocation());

		nicholas.setInvitedBy(winston);
		winston.setInvitedBy(nicholas);

		database.save(nicholas);
	}

	@Test(dependsOnMethods = "testSaveCircularLink")
	public void testQueryCircular() {
		List<Profile> result = database.query(new OSQLSynchQuery<ODocument>("select * from Profile"));

		Profile parent;
		for (Profile r : result) {

			System.out.println(r.getNick());

			parent = r.getInvitedBy();

			if (parent != null)
				System.out.println("- parent: " + parent.getName() + " " + parent.getSurname());
		}
	}

	@Test(dependsOnMethods = "testQueryCircular")
	public void testSaveMultiCircular() {
		startRecordNumber = database.countClusterElements("Profile");

		Profile bObama = database.newInstance(Profile.class, "ThePresident", "Barack", "Obama", null);
		bObama.setLocation(database.newInstance(Address.class, "Residence",
				database.newInstance(City.class, database.newInstance(Country.class, "Hawaii"), "Honolulu"), "unknown"));
		bObama.addFollower(database.newInstance(Profile.class, "PresidentSon1", "Malia Ann", "Obama", bObama));
		bObama.addFollower(database.newInstance(Profile.class, "PresidentSon2", "Natasha", "Obama", bObama));

		database.save(bObama);
	}

	@SuppressWarnings("unchecked")
	@Test(dependsOnMethods = "testSaveMultiCircular")
	public void testQueryMultiCircular() {
		Assert.assertEquals(database.countClusterElements("Profile"), startRecordNumber + 3);

		List<ODocument> result = database.getUnderlying()
				.command(new OSQLSynchQuery<ODocument>("select * from Profile where name = 'Barack' and surname = 'Obama'")).execute();

		Assert.assertEquals(result.size(), 1);

		for (ODocument profile : result) {

			System.out.println(profile.field("name") + " " + profile.field("surname"));

			final Collection<ODocument> followers = profile.field("followers");

			if (followers != null) {
				for (ODocument follower : followers) {
					Assert.assertTrue(((Collection<ODocument>) follower.field("followings")).contains(profile));

					System.out.println("- follower: " + follower.field("name") + " " + follower.field("surname") + " (parent: "
							+ follower.field("name") + " " + follower.field("surname") + ")");
				}
			}
		}
	}

	// @Test(dependsOnMethods = "testQueryMultiCircular")
	// public void testSetFieldSize() {
	// JavaComplexTestClass test = database.newInstance(JavaComplexTestClass.class);
	// for (int i = 0; i < 100; i++) {
	// Child child = database.newInstance(Child.class);
	// child.setName(String.valueOf(i));
	// test.getSet().add(child);
	// }
	// Assert.assertNotNull(test.getSet());
	// Assert.assertEquals(test.getSet().size(), 100);
	// database.save(test);
	// ORID rid = database.getIdentity(test);
	// close();
	// open();
	// test = database.load(rid);
	// Assert.assertNotNull(test.getSet());
	// Assert.assertEquals(test.getSet().size(), 100);
	// Iterator<Child> it = test.getSet().iterator();
	// for (int i = 0; i < 100; i++) {
	// Child child = it.next();
	// Assert.assertNotNull(child.getName());
	// Assert.assertTrue(Integer.valueOf(child.getName()) < 100);
	// Assert.assertTrue(Integer.valueOf(child.getName()) >= 0);
	// }
	// }

	@Test(dependsOnMethods = "testQueryMultiCircular")
	public void testCascadeDeleteSimpleObject() {
		JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
		JavaSimpleTestClass simple = database.newInstance(JavaSimpleTestClass.class);
		simple.setText("asdasd");
		test.setSimpleClass(simple);
		database.save(test);
		ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
		ORID simpleRid = database.getRecordByUserObject(simple, false).getIdentity();
		close();
		open();
		database.delete(testRid);
		simple = database.load(simpleRid);
		Assert.assertNull(simple);
	}

	@Test(dependsOnMethods = "testCascadeDeleteSimpleObject")
	public void testCascadeDeleteCollections() {
		JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
		Child listChild1 = database.newInstance(Child.class);
		listChild1.setName("list1");
		test.getList().add(listChild1);
		Child listChild2 = database.newInstance(Child.class);
		listChild2.setName("list2");
		test.getList().add(listChild2);
		Child listChild3 = database.newInstance(Child.class);
		listChild3.setName("list3");
		test.getList().add(listChild3);

		Child setChild1 = database.newInstance(Child.class);
		setChild1.setName("set1");
		test.getSet().add(setChild1);
		Child setChild2 = database.newInstance(Child.class);
		setChild2.setName("set2");
		test.getSet().add(setChild2);
		Child setChild3 = database.newInstance(Child.class);
		setChild3.setName("set3");
		test.getSet().add(setChild3);

		database.save(test);
		ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
		ORID list1Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
		ORID list2Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
		ORID list3Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
		ORID set1Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
		ORID set2Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
		ORID set3Rid = database.getRecordByUserObject(listChild1, false).getIdentity();
		close();
		open();
		database.delete(testRid);
		listChild1 = database.load(list1Rid);
		listChild2 = database.load(list2Rid);
		listChild3 = database.load(list3Rid);
		setChild1 = database.load(set1Rid);
		setChild2 = database.load(set2Rid);
		setChild3 = database.load(set3Rid);
		Assert.assertNull(listChild1);
		Assert.assertNull(listChild2);
		Assert.assertNull(listChild3);
		Assert.assertNull(setChild1);
		Assert.assertNull(setChild2);
		Assert.assertNull(setChild3);
	}

	@Test(dependsOnMethods = "testCascadeDeleteCollections")
	public void testCascadeDeleteMap() {
		JavaCascadeDeleteTestClass test = database.newInstance(JavaCascadeDeleteTestClass.class);
		Child mapChild1 = database.newInstance(Child.class);
		mapChild1.setName("map1");
		test.getChildren().put("1", mapChild1);
		Child mapChild2 = database.newInstance(Child.class);
		mapChild2.setName("map2");
		test.getChildren().put("2", mapChild2);
		Child mapChild3 = database.newInstance(Child.class);
		mapChild3.setName("map3");
		test.getChildren().put("3", mapChild3);

		database.save(test);
		ORID testRid = database.getRecordByUserObject(test, false).getIdentity();
		ORID map1Rid = database.getRecordByUserObject(mapChild1, false).getIdentity();
		ORID map2Rid = database.getRecordByUserObject(mapChild2, false).getIdentity();
		ORID map3Rid = database.getRecordByUserObject(mapChild3, false).getIdentity();

		close();
		open();

		database.delete(testRid);
		mapChild1 = database.load(map1Rid);
		mapChild2 = database.load(map2Rid);
		mapChild3 = database.load(map3Rid);
		Assert.assertNull(mapChild1);
		Assert.assertNull(mapChild2);
		Assert.assertNull(mapChild3);
	}

	@Test(dependsOnMethods = "testPool")
	public void testCustomTypes() {
		OObjectSerializerContext serializerContext = new OObjectSerializerContext();
		serializerContext.bind(new OObjectSerializer<CustomType, Long>() {

			public Long serializeFieldValue(Class<?> itype, CustomType iFieldValue) {
				serialized++;
				return iFieldValue.value;
			}

			public CustomType unserializeFieldValue(Class<?> itype, Long iFieldValue) {
				unserialized++;
				return new CustomType(iFieldValue);
			}

		});
		OObjectSerializerHelper.bindSerializerContext(null, serializerContext);
		database.getEntityManager().registerEntityClass(CustomClass.class);

		if (!database.getMetadata().getSchema().existsClass("CustomClass"))
			database.getMetadata().getSchema().createClass("CustomClass");

		List<CustomType> customTypesList = new ArrayList<CustomType>();
		customTypesList.add(new CustomType(102L));

		Set<CustomType> customTypeSet = new HashSet<CustomType>();
		customTypeSet.add(new CustomType(103L));

		Map<Long, CustomType> customTypeMap = new HashMap<Long, CustomType>();
		customTypeMap.put(1L, new CustomType(104L));

		CustomClass pojo = new CustomClass("test", 33L, new CustomType(101L), customTypesList, customTypeSet, customTypeMap);
		// init counters
		serialized = 0;
		unserialized = 0;
		pojo = database.save(pojo);
		Assert.assertEquals(serialized, 4);
		Assert.assertEquals(unserialized, 0);

		pojo = database.reload(pojo);
		Assert.assertEquals(unserialized, 0);

		pojo.getCustom();
		Assert.assertEquals(unserialized, 1);
		Assert.assertTrue(pojo.getCustom() instanceof CustomType);

		pojo.getCustomTypeList().iterator().next();
		Assert.assertEquals(unserialized, 2);
		Assert.assertTrue(pojo.getCustomTypeList().iterator().next() instanceof CustomType);
		unserialized--;

		pojo.getCustomTypeSet().iterator().next();
		Assert.assertEquals(unserialized, 3);
		Assert.assertTrue(pojo.getCustomTypeSet().iterator().next() instanceof CustomType);
		unserialized--;

		pojo.getCustomTypeMap().get(1L);
		Assert.assertEquals(serialized, 4);
		Assert.assertEquals(unserialized, 4);
		Assert.assertTrue(pojo.getCustomTypeMap().get(1L) instanceof CustomType);
	}

	@Test(dependsOnMethods = "testCustomTypes")
	public void testCustomTypesDatabaseNewInstance() {
		OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		ORID rid = null;
		try {
			// init counters
			serialized = 0;
			unserialized = 0;

			List<CustomType> customTypesList = new ArrayList<CustomType>();
			customTypesList.add(new CustomType(102L));

			Set<CustomType> customTypeSet = new HashSet<CustomType>();
			customTypeSet.add(new CustomType(103L));

			Map<Long, CustomType> customTypeMap = new HashMap<Long, CustomType>();
			customTypeMap.put(1L, new CustomType(104L));

			CustomClass pojo = database.newInstance(CustomClass.class, "test", 33L, new CustomType(101L), customTypesList, customTypeSet,
					customTypeMap);
			Assert.assertEquals(serialized, 4);
			Assert.assertEquals(unserialized, 0);

			pojo = database.save(pojo);

			rid = database.getIdentity(pojo);

			database.close();

			database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

			pojo = database.load(rid);
			Assert.assertEquals(unserialized, 0);

			pojo.getCustom();
			Assert.assertEquals(unserialized, 1);
			Assert.assertTrue(pojo.getCustom() instanceof CustomType);

			pojo.getCustomTypeList().iterator().next();
			Assert.assertEquals(unserialized, 2);
			Assert.assertTrue(pojo.getCustomTypeList().iterator().next() instanceof CustomType);
			unserialized--;

			pojo.getCustomTypeSet().iterator().next();
			Assert.assertEquals(unserialized, 3);
			Assert.assertTrue(pojo.getCustomTypeSet().iterator().next() instanceof CustomType);
			unserialized--;

			pojo.getCustomTypeMap().get(1L);
			Assert.assertEquals(serialized, 4);
			Assert.assertEquals(unserialized, 4);
			Assert.assertTrue(pojo.getCustomTypeMap().get(1L) instanceof CustomType);
		} finally {
			database.close();
		}
	}

	@Test(dependsOnMethods = "testCustomTypesDatabaseNewInstance")
	public void testEnumListWithCustomTypes() {
		OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		ORID rid = null;
		try {
			OObjectSerializerContext serializerContext = new OObjectSerializerContext();
			serializerContext.bind(new OObjectSerializer<SecurityRole, String>() {

				public Object serializeFieldValue(Class<?> type, SecurityRole role) {
					return role.name();
				}

				public Object unserializeFieldValue(Class<?> type, String str) {
					return SecurityRole.getByName(str);
				}
			});

			OObjectSerializerHelper.bindSerializerContext(null, serializerContext);

			database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.customserialization");

			Sec s = new Sec();
			s.getSecurityRoleList().add(SecurityRole.LOGIN);

			Assert.assertTrue(s.getSecurityRoleList().contains(SecurityRole.LOGIN));

			s = database.save(s);
			rid = database.getRecordByUserObject(s, false).getIdentity();

			database.close();

			database = OObjectDatabasePool.global().acquire(url, "admin", "admin");

			s = database.load(rid);

			Assert.assertTrue(s.getSecurityRoleList().contains(SecurityRole.LOGIN));
		} finally {
			database.close();
		}
	}

	@Test(dependsOnMethods = "testEnumListWithCustomTypes")
	public void childUpdateTest() {
		OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		Planet p = database.newInstance(Planet.class);
		Satellite sat = database.newInstance(Satellite.class);
		sat.setDiameter(50);
		p.addSatellite(sat);
		database.save(p);
		ORID rid = database.getIdentity(p);
		database.close();
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		p = database.load(rid);
		sat = p.getSatellites().get(0);
		Assert.assertEquals(sat.getDiameter(), 50);
		sat.setDiameter(500);
		// p.addSatellite(new Satellite("Moon", 70));
		// db.save(sat);
		database.save(p);
		database.close();
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		p = database.load(rid);
		sat = p.getSatellites().get(0);
		Assert.assertEquals(sat.getDiameter(), 500);
		database.close();
	}

	@Test(dependsOnMethods = "childUpdateTest")
	public void childNLevelUpdateTest() {
		OObjectDatabaseTx database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		Planet p = database.newInstance(Planet.class);
		Planet near = database.newInstance(Planet.class);
		Satellite sat = database.newInstance(Satellite.class);
		Satellite satNear = database.newInstance(Satellite.class);
		sat.setDiameter(50);
		sat.setNear(near);
		satNear.setDiameter(10);
		near.addSatellite(satNear);
		p.addSatellite(sat);
		database.save(p);
		ORID rid = database.getIdentity(p);
		database.close();
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		p = database.load(rid);
		sat = p.getSatellites().get(0);
		near = sat.getNear();
		satNear = near.getSatellites().get(0);
		Assert.assertEquals(satNear.getDiameter(), 10);
		satNear.setDiameter(100);
		// p.addSatellite(new Satellite("Moon", 70));
		// db.save(sat);
		database.save(p);
		database.close();
		database = OObjectDatabasePool.global().acquire(url, "admin", "admin");
		p = database.load(rid);
		sat = p.getSatellites().get(0);
		near = sat.getNear();
		satNear = near.getSatellites().get(0);
		Assert.assertEquals(satNear.getDiameter(), 100);
		database.close();
	}
}
