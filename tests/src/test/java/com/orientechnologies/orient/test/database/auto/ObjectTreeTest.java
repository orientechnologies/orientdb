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

import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.domain.business.Address;
import com.orientechnologies.orient.test.domain.business.City;
import com.orientechnologies.orient.test.domain.business.Country;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "record-object" }, sequential = true)
public class ObjectTreeTest {
	private ODatabaseObjectTx	database;
	protected long						startRecordNumber;

	@Parameters(value = "url")
	public ObjectTreeTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());

		database = new ODatabaseObjectTx(iURL);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test
	public void testPersonSaving() {
		database.open("admin", "admin");

		final long beginProfiles = database.countClusterElements("Profile");

		Country italy = new Country("Italy");

		Profile garibaldi = new Profile("GGaribaldi", "Giuseppe", "Garibaldi", null);
		garibaldi.setLocation(new Address("Residence", new City(italy, "Rome"), "Piazza Navona, 1"));
		database.save(garibaldi);

		Profile bonaparte = new Profile("NBonaparte", "Napoleone", "Bonaparte", garibaldi);
		bonaparte.setLocation(new Address("Residence", garibaldi.getLocation().getCity(), "Piazza di Spagna, 111"));
		database.save(bonaparte);

		Assert.assertEquals(database.countClusterElements("Profile"), beginProfiles + 2);
	}

	@Test(dependsOnMethods = "testPersonSaving")
	public void testCitySaving() {
		Assert.assertEquals(database.countClusterElements("City"), 1);
	}

	@Test(dependsOnMethods = "testCitySaving")
	public void testCityEquality() {
		Iterator<Profile> iter = database.browseClass(Profile.class);

		Profile p1 = iter.next();
		Profile p2 = iter.next();

		Assert.assertNotSame(p1, p2);
		Assert.assertEquals(p1.getLocation().getCity(), p2.getLocation().getCity());
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

			System.out.println(r.getName() + " " + r.getSurname());

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

		List<ODocument> result = database.getUnderlying().command(
				new OSQLSynchQuery<ODocument>("select * from Profile where name = 'Barack' and surname = 'Obama'")).execute();

		Assert.assertEquals(result.size(), 1);

		List<ODocument> followers;
		for (ODocument profile : result) {

			System.out.println(profile.field("name") + " " + profile.field("surname"));

			followers = profile.field("followers");

			if (followers != null) {
				for (ODocument follower : followers) {
					Assert.assertTrue(((Collection<ODocument>) follower.field("followings")).contains(profile));

					System.out.println("- follower: " + follower.field("name") + " " + follower.field("surname") + " (parent: "
							+ follower.field("name") + " " + follower.field("surname") + ")");
				}
			}
		}
	}

	@Test(dependsOnMethods = "testQueryMultiCircular")
	public void close() {
		database.close();
	}
}
