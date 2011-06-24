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
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.database.base.OrientTest;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "index" })
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

		} catch (OIndexException e) {
			Assert.assertTrue(true);
		}
		database.close();
	}

	@Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
	public void testUseOfIndex() {
		database.open("admin", "admin");

		final List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick = 'Jay'"))
				.execute();

		Assert.assertFalse(result.isEmpty());

		Profile record;
		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getName().toString().equalsIgnoreCase("Jay"));
		}

		database.close();
	}

	@Test(dependsOnMethods = "testUseOfIndex")
	public void testChangeOfIndexToNotUnique() {
		database.open("admin", "admin");
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndex();
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(INDEX_TYPE.NOTUNIQUE);
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
	public void testQueryIndex() {
		database.open("admin", "admin");

		List<?> result = database.query(new OSQLSynchQuery<Object>("select from index:profile.nick where key = 'Jay'"));
		Assert.assertTrue(result.size() > 0);

		database.close();
	}

	@Test
	public void testIndexSQL() {
		database.open("admin", "admin");

		database.command(new OCommandSQL("create index idx unique")).execute();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("idx"));

		database.command(new OCommandSQL("insert into index:IDX (key,rid) values (10,#3:0)")).execute();
		database.command(new OCommandSQL("insert into index:IDX (key,rid) values (20,#3:1)")).execute();

		List<ODocument> result = database.command(new OCommandSQL("select from index:IDX")).execute();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 2);
		for (ODocument d : result) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (d.field("key").equals(10))
				Assert.assertEquals(d.rawField("rid"), new ORecordId("#3:0"));
			else if (d.field("key").equals(20))
				Assert.assertEquals(d.rawField("rid"), new ORecordId("#3:1"));
			else
				Assert.assertTrue(false);
		}

		result = database.command(new OCommandSQL("select key, rid from index:IDX")).execute();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 2);
		for (ODocument d : result) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));

			if (d.field("key").equals(10))
				Assert.assertEquals(d.rawField("rid"), new ORecordId("#3:0"));
			else if (d.field("key").equals(20))
				Assert.assertEquals(d.rawField("rid"), new ORecordId("#3:1"));
			else
				Assert.assertTrue(false);
		}

		result = database.command(new OCommandSQL("select key from index:IDX")).execute();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 2);
		for (ODocument d : result) {
			Assert.assertTrue(d.containsField("key"));
			Assert.assertFalse(d.containsField("rid"));
		}

		result = database.command(new OCommandSQL("select rid from index:IDX")).execute();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 2);
		for (ODocument d : result) {
			Assert.assertFalse(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));
		}

		result = database.command(new OCommandSQL("select rid from index:IDX where key = 10")).execute();
		Assert.assertNotNull(result);
		Assert.assertEquals(result.size(), 1);
		for (ODocument d : result) {
			Assert.assertFalse(d.containsField("key"));
			Assert.assertTrue(d.containsField("rid"));
		}

		database.close();
	}

	@Test(dependsOnMethods = "testQueryIndex")
	public void testChangeOfIndexToUnique() {
		database.open("admin", "admin");
		try {
			database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndex();
			database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(INDEX_TYPE.UNIQUE);
			Assert.assertTrue(false);
		} catch (OIndexException e) {
			Assert.assertTrue(true);
		}

		database.close();
	}

}
