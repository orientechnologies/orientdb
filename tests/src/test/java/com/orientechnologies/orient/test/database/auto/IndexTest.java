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

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.database.base.OrientTest;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "index" }, sequential = true)
public class IndexTest {
	private ODatabaseObjectTx	database;
	protected long						startRecordNumber;

	@Parameters(value = "url")
	public IndexTest(String iURL) {
		Orient.instance().registerEngine(new OEngineRemote());

		database = new ODatabaseObjectTx(iURL);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test
	public void testDuplicatedIndexOnUnique() {
		database.open("admin", "admin");

		Profile jayMiner = new Profile("Jay", "Jay", "Miner", null);
		database.save(jayMiner);

		Profile jacobMiner = new Profile("Jay", "Jacob", "Miner", null);

		try {
			database.save(jacobMiner);

			// IT SHOULD GIVE ERROR ON DUPLICATED KEY
			Assert.assertTrue(false);

		} catch (ODatabaseException e) {
			Assert.assertTrue(e.getCause() instanceof OIndexException);
		}
		database.close();
	}

	@Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
	public void testUseOfIndex() {
		database.open("admin", "admin");

		final List<ODocument> result = database.command(new OSQLSynchQuery<ODocument>("select * from Profile where nick = 'Jay'"))
				.execute();

		ODocument record;
		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getClassName().equalsIgnoreCase("Profile"));
			Assert.assertTrue(record.field("name").toString().equalsIgnoreCase("Jay"));
		}

		database.close();
	}

	@Test(dependsOnMethods = "testUseOfIndex")
	public void testChangeOfIndexToNotUnique() {
		database.open("admin", "admin");
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getIndex().setUnique(false);
		database.getMetadata().getSchema().save();
		database.close();
	}

	@Test(dependsOnMethods = "testChangeOfIndexToNotUnique")
	public void testDuplicatedIndexOnNotUnique() {
		database.open("admin", "admin");

		Profile nickNolte = new Profile("Jay", "Nick", "Nolte", null);
		database.save(nickNolte);

		database.close();
	}

	@Test(dependsOnMethods = "testDuplicatedIndexOnNotUnique")
	public void testChangeOfIndexToUnique() {
		database.open("admin", "admin");
		try {
			database.getMetadata().getSchema().getClass("Profile").getProperty("nick").getIndex().setUnique(true);
			Assert.assertTrue(false);
		} catch (OIndexException e) {
		}

		database.close();
	}
}
