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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.object.ODatabaseObjectPool;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializer;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerContext;
import com.orientechnologies.orient.core.serialization.serializer.object.OObjectSerializerHelper;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "record-object" })
public class ObjectTreeTest {
	private ODatabaseObjectTx	database;
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

		public CustomClass(String iName, Long iAge, CustomType iCustom, List<CustomType> iCustomTypeList,
				Set<CustomType> iCustomTypeSet, Map<Long, CustomType> iCustomTypeMap) {
			name = iName;
			age = iAge;
			custom = iCustom;
			customTypeList = iCustomTypeList;
			customTypeSet = iCustomTypeSet;
			customTypeMap = iCustomTypeMap;
		}
	}

	public class CustomType {
		public long	value;

		public CustomType(Long iFieldValue) {
			value = iFieldValue;
		}
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
		database = new ODatabaseObjectTx(url);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
		if ("memory:test".equals(database.getURL())) {
			database.create();
		} else {
			database.open("admin", "admin");
		}
	}

	@Test
	public void testPool() throws IOException {
		final ODatabaseObjectTx[] dbs = new ODatabaseObjectTx[ODatabaseObjectPool.global().getMaxSize()];

		for (int i = 0; i < 10; ++i) {
			for (int db = 0; db < dbs.length; ++db)
				dbs[db] = ODatabaseObjectPool.global().acquire(url, "admin", "admin");
			for (int db = 0; db < dbs.length; ++db)
				dbs[db].close();
		}
	}

	@Test
	public void testPersonSaving() {
		final long beginProfiles = database.countClusterElements("Profile");
		beginCities = database.countClusterElements("City");

		Country italy = new Country("Italy");

		Profile garibaldi = new Profile("GGaribaldi", "Giuseppe", "Garibaldi", null);
		garibaldi.setLocation(new Address("Residence", new City(italy, "Rome"), "Piazza Navona, 1"));

		Profile bonaparte = new Profile("NBonaparte", "Napoleone", "Bonaparte", garibaldi);
		bonaparte.setLocation(new Address("Residence", garibaldi.getLocation().getCity(), "Piazza di Spagna, 111"));
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
		Assert.assertSame(p1.getLocation().getCity(), p2.getLocation().getCity());
	}

	@Test(dependsOnMethods = "testCityEquality")
	public void testSaveCircularLink() {
		Profile winston = new Profile("WChurcill", "Winston", "Churcill", null);
		winston.setLocation(new Address("Residence", new City(new Country("England"), "London"), "unknown"));

		Profile nicholas = new Profile("NChurcill", "Nicholas ", "Churcill", winston);
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

		Profile bObama = new Profile("ThePresident", "Barack", "Obama", null);
		bObama.setLocation(new Address("Residence", new City(new Country("Hawaii"), "Honolulu"), "unknown"));
		bObama.addFollower(new Profile("PresidentSon1", "Malia Ann", "Obama", bObama));
		bObama.addFollower(new Profile("PresidentSon2", "Natasha", "Obama", bObama));

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

	@Test
	public void testCustomTypes() {
		database.getEntityManager().registerEntityClass(CustomClass.class);

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
		database.save(pojo);
		Assert.assertEquals(serialized, 4);
		Assert.assertEquals(unserialized, 0);

		database.reload(pojo);
		Assert.assertEquals(serialized, 4);
		Assert.assertEquals(unserialized, 4);
	}
}
