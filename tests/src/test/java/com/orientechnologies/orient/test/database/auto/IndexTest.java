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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import com.orientechnologies.orient.core.metadata.schema.OProperty;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.db.object.ODatabaseObjectTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.metadata.schema.OProperty.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.test.database.base.OrientTest;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "index" })
public class IndexTest {
	private ODatabaseObjectTx	database;
	protected long						startRecordNumber;

	@BeforeMethod
	public void beforeMethod() {
		database.open("admin", "admin");
	}

	@AfterMethod
	public void afterMethod() {
		database.close();
	}

	@Parameters(value = "url")
	public IndexTest(String iURL) {
		database = new ODatabaseObjectTx(iURL);
		database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain");
	}

	@Test(dependsOnMethods = "testIndexGetValuesUniqueIndex")
	public void testDuplicatedIndexOnUnique() {
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
	}

    @Test(dependsOnMethods = "populateIndexDocuments")
    public void testIndexInUniqueIndex() {
        final OProperty nickProperty = database.getMetadata().getSchema().getClass("Profile").getProperty("nick");
        Assert.assertEquals(nickProperty.getIndex().getUnderlying().getType(), INDEX_TYPE.UNIQUE.toString());

        final boolean localStorage = !(database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread);

        boolean oldRecording = true;
        long indexQueries = 0L;
        if (localStorage) {
            oldRecording = OProfiler.getInstance().isRecording();

            if (!oldRecording) {
                OProfiler.getInstance().startRecording();
            }

            indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
            if (indexQueries < 0) {
                indexQueries = 0;
            }
        }

        final List<Profile> result = database.command(
                new OSQLSynchQuery<Profile>("SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0' ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']")).execute();

		final List<String> expectedSurnames = new ArrayList<String>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

		if ( localStorage && !oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 3);
		for (final Profile profile : result) {
			expectedSurnames.remove(profile.getSurname());
		}

		Assert.assertEquals(expectedSurnames.size(), 0);

        if (localStorage) {
            final long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
            Assert.assertEquals(newIndexQueries, indexQueries + 1);
        }
    }


	@Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
	public void testUseOfIndex() {
		final List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick = 'Jay'"))
				.execute();

		Assert.assertFalse(result.isEmpty());

		Profile record;
		for (int i = 0; i < result.size(); ++i) {
			record = result.get(i);

			OrientTest.printRecord(i, record);

			Assert.assertTrue(record.getName().toString().equalsIgnoreCase("Jay"));
		}
	}

	@Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
	public void testIndexEntries() {
		List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick is not null")).execute();

		OIndex<?> idx = database.getMetadata().getIndexManager().getIndex("Profile.nick");

		Assert.assertEquals(idx.getSize(), result.size());
	}

	@Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
	public void testIndexSize() {
		List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick is not null")).execute();

		int profileSize = result.size();

		database.getMetadata().getIndexManager().reload();
		Assert.assertEquals(database.getMetadata().getIndexManager().getIndex("Profile.nick").getSize(), profileSize);
		for (int i = 0; i < 10; i++) {
			Profile profile = new Profile("Yay-" + i, "Jay", "Miner", null);
			database.save(profile);
			profileSize++;
			Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("Profile.nick").get("Yay-" + i));
		}
	}

	@Test(dependsOnMethods = "testUseOfIndex")
	public void testChangeOfIndexToNotUnique() {
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndex();
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(INDEX_TYPE.NOTUNIQUE);
	}

	@Test(dependsOnMethods = "testChangeOfIndexToNotUnique")
	public void testDuplicatedIndexOnNotUnique() {
		Profile nickNolte = new Profile("Jay", "Nick", "Nolte", null);
		database.save(nickNolte);
	}

	@Test(dependsOnMethods = "testDuplicatedIndexOnNotUnique")
	public void testQueryIndex() {
		List<?> result = database.query(new OSQLSynchQuery<Object>("select from index:profile.nick where key = 'Jay'"));
		Assert.assertTrue(result.size() > 0);
	}

	@Test
	public void testIndexSQL() {
		database.command(new OCommandSQL("create index idx unique")).execute();
		database.getMetadata().getIndexManager().reload();
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
	}

	@Test(dependsOnMethods = "testQueryIndex")
	public void testChangeOfIndexToUnique() {
		try {
			database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndex();
			database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(INDEX_TYPE.UNIQUE);
			Assert.assertTrue(false);
		} catch (OIndexException e) {
			Assert.assertTrue(true);
		}
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testValuesMajor() {
		database.command(new OCommandSQL("create index equalityIdx unique")).execute();

		database.getMetadata().getIndexManager().reload();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("equalityIdx"));

		for (int key = 0; key <= 5; key++) {
			database.command(new OCommandSQL("insert into index:equalityIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
		}

		final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("equalityIdx");

		final Collection<Long> valuesMajorResults = new ArrayList<Long>(Arrays.asList(4L, 5L));
		Collection<OIdentifiable> indexCollection = index.getValuesMajor(3, false);
		Assert.assertEquals(indexCollection.size(), 2);
		for (OIdentifiable identifiable : indexCollection) {
			valuesMajorResults.remove(identifiable.getIdentity().getClusterPosition());
		}
		Assert.assertEquals(valuesMajorResults.size(), 0);

		final Collection<Long> valuesMajorInclusiveResults = new ArrayList<Long>(Arrays.asList(3L, 4L, 5L));
		indexCollection = index.getValuesMajor(3, true);
		Assert.assertEquals(indexCollection.size(), 3);
		for (OIdentifiable identifiable : indexCollection) {
			valuesMajorInclusiveResults.remove(identifiable.getIdentity().getClusterPosition());
		}
		Assert.assertEquals(valuesMajorInclusiveResults.size(), 0);

		indexCollection = index.getValuesMajor(5, true);
		Assert.assertEquals(indexCollection.size(), 1);
		Assert.assertEquals(indexCollection.iterator().next().getIdentity().getClusterPosition(), 5L);

		indexCollection = index.getValuesMajor(5, false);
		Assert.assertEquals(indexCollection.size(), 0);

		database.command(new OCommandSQL("drop index equalityIdx")).execute();
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testEntriesMajor() {
		database.command(new OCommandSQL("create index equalityIdx unique")).execute();

		database.getMetadata().getIndexManager().reload();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("equalityIdx"));

		for (int key = 0; key <= 5; key++) {
			database.command(new OCommandSQL("insert into index:equalityIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
		}

		final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("equalityIdx");

		final Collection<Integer> valuesMajorResults = new ArrayList<Integer>(Arrays.asList(4, 5));
		Collection<ODocument> indexCollection = index.getEntriesMajor(3, false);
		Assert.assertEquals(indexCollection.size(), 2);
		for (ODocument doc : indexCollection) {
			valuesMajorResults.remove(doc.<Integer> field("key"));
			Assert.assertEquals(doc.<ORecordId> rawField("rid"), new ORecordId(10, doc.<Integer> field("key").longValue()));
		}
		Assert.assertEquals(valuesMajorResults.size(), 0);

		final Collection<Integer> valuesMajorInclusiveResults = new ArrayList<Integer>(Arrays.asList(3, 4, 5));
		indexCollection = index.getEntriesMajor(3, true);
		Assert.assertEquals(indexCollection.size(), 3);
		for (ODocument doc : indexCollection) {
			valuesMajorInclusiveResults.remove(doc.<Integer> field("key"));
			Assert.assertEquals(doc.<ORecordId> rawField("rid"), new ORecordId(10, doc.<Integer> field("key").longValue()));
		}
		Assert.assertEquals(valuesMajorInclusiveResults.size(), 0);

		indexCollection = index.getEntriesMajor(5, true);
		Assert.assertEquals(indexCollection.size(), 1);
		Assert.assertEquals(indexCollection.iterator().next().<Integer> field("key"), Integer.valueOf(5));
		Assert.assertEquals(indexCollection.iterator().next().<ORecordId> rawField("rid"), new ORecordId(10, 5));

		indexCollection = index.getEntriesMajor(5, false);
		Assert.assertEquals(indexCollection.size(), 0);

		database.command(new OCommandSQL("drop index equalityIdx")).execute();
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testValuesMinor() {
		database.command(new OCommandSQL("create index equalityIdx unique")).execute();

		database.getMetadata().getIndexManager().reload();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("equalityIdx"));

		for (int key = 0; key <= 5; key++) {
			database.command(new OCommandSQL("insert into index:equalityIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
		}

		final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("equalityIdx");

		final Collection<Long> valuesMinorResults = new ArrayList<Long>(Arrays.asList(0L, 1L, 2L));
		Collection<OIdentifiable> indexCollection = index.getValuesMinor(3, false);
		Assert.assertEquals(indexCollection.size(), 3);
		for (OIdentifiable identifiable : indexCollection) {
			valuesMinorResults.remove(identifiable.getIdentity().getClusterPosition());
		}
		Assert.assertEquals(valuesMinorResults.size(), 0);

		final Collection<Long> valuesMinorInclusiveResults = new ArrayList<Long>(Arrays.asList(0L, 1L, 2L, 3L));
		indexCollection = index.getValuesMinor(3, true);
		Assert.assertEquals(indexCollection.size(), 4);
		for (OIdentifiable identifiable : indexCollection) {
			valuesMinorInclusiveResults.remove(identifiable.getIdentity().getClusterPosition());
		}
		Assert.assertEquals(valuesMinorInclusiveResults.size(), 0);

		indexCollection = index.getValuesMinor(0, true);
		Assert.assertEquals(indexCollection.size(), 1);
		Assert.assertEquals(indexCollection.iterator().next().getIdentity().getClusterPosition(), 0L);

		indexCollection = index.getValuesMinor(0, false);
		Assert.assertEquals(indexCollection.size(), 0);

		database.command(new OCommandSQL("drop index equalityIdx")).execute();
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testEntriesMinor() {
		database.command(new OCommandSQL("create index equalityIdx unique")).execute();

		database.getMetadata().getIndexManager().reload();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("equalityIdx"));

		for (int key = 0; key <= 5; key++) {
			database.command(new OCommandSQL("insert into index:equalityIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
		}

		final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("equalityIdx");

		final Collection<Integer> valuesMinorResults = new ArrayList<Integer>(Arrays.asList(0, 1, 2));
		Collection<ODocument> indexCollection = index.getEntriesMinor(3, false);
		Assert.assertEquals(indexCollection.size(), 3);
		for (ODocument doc : indexCollection) {
			valuesMinorResults.remove(doc.<Integer> field("key"));
			Assert.assertEquals(doc.<ORecordId> rawField("rid"), new ORecordId(10, doc.<Integer> field("key").longValue()));
		}
		Assert.assertEquals(valuesMinorResults.size(), 0);

		final Collection<Integer> valuesMinorInclusiveResults = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3));
		indexCollection = index.getEntriesMinor(3, true);
		Assert.assertEquals(indexCollection.size(), 4);
		for (ODocument doc : indexCollection) {
			valuesMinorInclusiveResults.remove(doc.<Integer> field("key"));
			Assert.assertEquals(doc.<ORecordId> rawField("rid"), new ORecordId(10, doc.<Integer> field("key").longValue()));
		}
		Assert.assertEquals(valuesMinorInclusiveResults.size(), 0);

		indexCollection = index.getEntriesMinor(0, true);
		Assert.assertEquals(indexCollection.size(), 1);
		Assert.assertEquals(indexCollection.iterator().next().<Integer> field("key"), Integer.valueOf(0));
		Assert.assertEquals(indexCollection.iterator().next().<ORecordId> rawField("rid"), new ORecordId(10, 0));

		indexCollection = index.getEntriesMinor(0, false);
		Assert.assertEquals(indexCollection.size(), 0);

		database.command(new OCommandSQL("drop index equalityIdx")).execute();
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testBetweenEntries() {
		database.command(new OCommandSQL("create index equalityIdx unique integer")).execute();

		database.getMetadata().getIndexManager().reload();
		Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("equalityIdx"));

		for (int key = 0; key <= 5; key++) {
			database.command(new OCommandSQL("insert into index:equalityIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
		}

		final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("equalityIdx");

		final Collection<Integer> betweenResults = new ArrayList<Integer>(Arrays.asList(1, 2, 3));
		Collection<ODocument> indexCollection = index.getEntriesBetween(1, 3);
		Assert.assertEquals(indexCollection.size(), 3);
		for (ODocument doc : indexCollection) {
			betweenResults.remove(doc.<Integer> field("key"));
			Assert.assertEquals(doc.<ORecordId> rawField("rid"), new ORecordId(10, doc.<Integer> field("key").longValue()));
		}
		Assert.assertEquals(betweenResults.size(), 0);

		database.command(new OCommandSQL("drop index equalityIdx")).execute();
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexInMajorSelect() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database.command(
				new OSQLSynchQuery<Profile>("select * from Profile where nick > 'ZZZJayLongNickIndex3'")).execute();
		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 2);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries + 1);
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexInMajorEqualsSelect() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database.command(
				new OSQLSynchQuery<Profile>("select * from Profile where nick >= 'ZZZJayLongNickIndex3'")).execute();
		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4",
				"ZZZJayLongNickIndex5"));

		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 3);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries + 1);
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexInMinorSelect() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick < '002'"))
				.execute();
		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("000", "001"));

		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 2);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries + 1);
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexInMinorEqualsSelect() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick <= '002'"))
				.execute();
		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("000", "001", "002"));

		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 3);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries + 1);
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexBetweenSelect() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database.command(
				new OSQLSynchQuery<Profile>("select * from Profile where nick between '001' and '004'")).execute();
		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("001", "002", "003", "004"));

		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 4);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries + 1);
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexInComplexSelectOne() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database.command(
				new OSQLSynchQuery<Profile>("select * from Profile where (name = 'Giuseppe' OR name <> 'Napoleone')"
						+ " AND (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND (nick >= 'ZZZJayLongNickIndex3'))"))
				.execute();
		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4",
				"ZZZJayLongNickIndex5"));
		Assert.assertEquals(result.size(), 3);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries + 1);
	}

	@Test(dependsOnMethods = "populateIndexDocuments")
	public void testIndexInComplexSelectTwo() {
		if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
			return;
		}

		final boolean oldRecording = OProfiler.getInstance().isRecording();

		if (!oldRecording) {
			OProfiler.getInstance().startRecording();
		}

		long indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		if (indexQueries < 0) {
			indexQueries = 0;
		}

		final List<Profile> result = database
				.command(
						new OSQLSynchQuery<Profile>(
								"select * from Profile where "
										+ "((name = 'Giuseppe' OR name <> 'Napoleone')"
										+ " AND (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND (nick >= 'ZZZJayLongNickIndex3' OR nick >= 'ZZZJayLongNickIndex4')))"))
				.execute();
		if (!oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4",
				"ZZZJayLongNickIndex5"));
		Assert.assertEquals(result.size(), 3);
		for (Profile profile : result) {
			expectedNicks.remove(profile.getNick());
		}

		Assert.assertEquals(expectedNicks.size(), 0);
		long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
		Assert.assertEquals(newIndexQueries, indexQueries);
	}

	public void populateIndexDocuments() {
		for (int i = 0; i <= 5; i++) {
			final Profile profile = new Profile("ZZZJayLongNickIndex" + i, "NickIndex" + i, "NolteIndex" + i, null);
			database.save(profile);
		}

		for (int i = 0; i <= 5; i++) {
			final Profile profile = new Profile("00" + i, "NickIndex" + i, "NolteIndex" + i, null);
			database.save(profile);
		}
	}

	@Test(dependsOnMethods = "testChangeOfIndexToUnique")
	public void removeNotUniqueIndexOnNick() {
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndex();
		database.getMetadata().getSchema().save();
	}

	@Test(dependsOnMethods = "removeNotUniqueIndexOnNick")
	public void testQueryingWithoutNickIndex() {
		Assert.assertTrue(database.getMetadata().getSchema().getClass("Profile").getProperty("name").isIndexed());
		Assert.assertTrue(!database.getMetadata().getSchema().getClass("Profile").getProperty("nick").isIndexed());

		List<Profile> result = database.command(new OSQLSynchQuery<ODocument>("SELECT FROM Profile WHERE nick = 'Jay'")).execute();
		Assert.assertEquals(result.size(), 2);

		result = database.command(new OSQLSynchQuery<ODocument>("SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Jay'")).execute();
		Assert.assertEquals(result.size(), 1);

		result = database.command(new OSQLSynchQuery<ODocument>("SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Nick'")).execute();
		Assert.assertEquals(result.size(), 1);
	}

	@Test(dependsOnMethods = "testQueryingWithoutNickIndex")
	public void createNotUniqueIndexOnNick() {
		database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(INDEX_TYPE.NOTUNIQUE);
		database.getMetadata().getSchema().save();
	}

    @Test(dependsOnMethods = {"createNotUniqueIndexOnNick", "populateIndexDocuments"})
    public void testIndexInNotUniqueIndex() {
        final OProperty nickProperty = database.getMetadata().getSchema().getClass("Profile").getProperty("nick");
        Assert.assertEquals(nickProperty.getIndex().getUnderlying().getType(), INDEX_TYPE.NOTUNIQUE.toString());

        final boolean localStorage = !(database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread);

        boolean oldRecording = true;
        long indexQueries = 0L;
        if (localStorage) {
            oldRecording = OProfiler.getInstance().isRecording();

            if (!oldRecording) {
                OProfiler.getInstance().startRecording();
            }

            indexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
            if (indexQueries < 0) {
                indexQueries = 0;
            }
        }

        final List<Profile> result = database.command(
                new OSQLSynchQuery<Profile>("SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0' ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']")).execute();

		final List<String> expectedSurnames = new ArrayList<String>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

		if ( localStorage && !oldRecording) {
			OProfiler.getInstance().stopRecording();
		}

		Assert.assertEquals(result.size(), 3);
		for (final Profile profile : result) {
			expectedSurnames.remove(profile.getSurname());
		}

		Assert.assertEquals(expectedSurnames.size(), 0);

        if (localStorage) {
            final long newIndexQueries = OProfiler.getInstance().getCounter("Query.indexUsage");
            Assert.assertEquals(newIndexQueries, indexQueries + 1);
        }
    }

    @Test(dependsOnMethods = "populateIndexDocuments")
    public void testIndexGetValuesUniqueIndex() {
        database.command(new OCommandSQL("create index inIdx unique")).execute();

        database.getMetadata().getIndexManager().reload();
        Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("inIdx"));

        for (int key = 0; key <= 5; key++) {
            database.command(new OCommandSQL("insert into index:inIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
        }

        final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("inIdx");
        final Collection<Integer> multiGetResults = new ArrayList<Integer>(Arrays.asList(1, 3));
        final Collection<OIdentifiable> indexCollection = index.getValues(Arrays.asList(1, 3));
        Assert.assertEquals(indexCollection.size(), 2);
        for (final OIdentifiable identifiable : indexCollection) {
            multiGetResults.remove(Long.valueOf(identifiable.getIdentity().getClusterPosition()).intValue());
        }
        Assert.assertEquals(multiGetResults.size(), 0);

        database.command(new OCommandSQL("drop index inIdx")).execute();
    }

    @Test(dependsOnMethods = "populateIndexDocuments")
    public void testIndexGetValuesNotUniqueIndex() {
        database.command(new OCommandSQL("create index inIdx notunique")).execute();

        database.getMetadata().getIndexManager().reload();
        Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("inIdx"));

        for(int i = 0; i < 2; i++)
            for (int key = 0; key <= 2; key++) {
                database.command(new OCommandSQL("insert into index:inIdx (key,rid) values (" + key + ",#10:" + (i + key * 2)  + ")")).execute();
            }

        final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("inIdx");
        final Collection<Integer> multiGetResults = new ArrayList<Integer>(Arrays.asList(0, 1, 4, 5));
        final Collection<OIdentifiable> indexCollection = index.getValues(Arrays.asList(0, 2));
        Assert.assertEquals(indexCollection.size(), 4);
        for (final OIdentifiable identifiable : indexCollection) {
            multiGetResults.remove(Long.valueOf(identifiable.getIdentity().getClusterPosition()).intValue());
        }
        Assert.assertEquals(multiGetResults.size(), 0);

        database.command(new OCommandSQL("drop index inIdx")).execute();
    }

    @Test(dependsOnMethods = "populateIndexDocuments")
    public void testIndexGetEntriesUniqueIndex() {
        database.command(new OCommandSQL("create index inIdx unique")).execute();

        database.getMetadata().getIndexManager().reload();
        Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("inIdx"));

        for (int key = 0; key <= 5; key++) {
            database.command(new OCommandSQL("insert into index:inIdx (key,rid) values (" + key + ",#10:" + key + ")")).execute();
        }

        final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("inIdx");
        final Collection<Integer> multiGetResults = new ArrayList<Integer>(Arrays.asList(1, 3));
        final Collection<ODocument> indexCollection = index.getEntries(Arrays.asList(1, 3));
        Assert.assertEquals(indexCollection.size(), 2);
        for (final ODocument doc : indexCollection) {
            multiGetResults.remove(doc.<Integer>field("key"));
        }
        Assert.assertEquals(multiGetResults.size(), 0);

        database.command(new OCommandSQL("drop index inIdx")).execute();
    }

    @Test(dependsOnMethods = "populateIndexDocuments")
    public void testIndexGetEntriesNotUniqueIndex() {
        database.command(new OCommandSQL("create index inIdx notunique")).execute();

        database.getMetadata().getIndexManager().reload();
        Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("inIdx"));

        for(int i = 0; i < 2; i++)
            for (int key = 0; key <= 2; key++) {
                database.command(new OCommandSQL("insert into index:inIdx (key,rid) values (" + key + ",#10:" + (i + key * 2)  + ")")).execute();
            }

        final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("inIdx");
        final Collection<Integer> multiGetResults = new ArrayList<Integer>(Arrays.asList(0,0, 2, 2));
        final Collection<ODocument> indexCollection = index.getEntries(Arrays.asList(0, 2));
        Assert.assertEquals(indexCollection.size(), 4);
        for (final ODocument doc : indexCollection) {
            multiGetResults.remove(doc.<Integer>field("key"));
        }
        Assert.assertEquals(multiGetResults.size(), 0);

        database.command(new OCommandSQL("drop index inIdx")).execute();
    }


	@SuppressWarnings("unchecked")
	public void LongTypes() {
		database.getMetadata().getSchema().getClass("Profile").createProperty("hash", OType.LONG).createIndex(INDEX_TYPE.UNIQUE);

		OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) database.getMetadata().getIndexManager().getIndex("Profile.hash");

		for (int i = 0; i < 5; i++) {
			Profile profile = new Profile("HashTest1").setHash(100l + i);
			database.save(profile);
		}

		Iterator<Entry<Object, OIdentifiable>> it = idx.iterator();
		while (it.hasNext()) {
			it.next();
		}

		Assert.assertEquals(idx.getSize(), 5);
	}

	public void indexLinks() {
		database.getMetadata().getSchema().getClass("Whiz").getProperty("account").createIndex(INDEX_TYPE.NOTUNIQUE);

		final List<Account> result = database.command(new OSQLSynchQuery<Profile>("select * from Account limit 1")).execute();

		// Assert.assertEquals(result.size(), 1);

		OIndex<?> idx = database.getMetadata().getIndexManager().getIndex("Whiz.account");

		for (int i = 0; i < 5; i++) {
			ODocument whiz = new ODocument(database.getUnderlying(), "Whiz");

			whiz.field("id", i);
			whiz.field("text", "This is a test");
			whiz.field("account", result.get(0).getRid());

			whiz.save();
		}

		Assert.assertEquals(idx.getSize(), 1);

		List<ODocument> indexedResult = database.getUnderlying()
				.command(new OSQLSynchQuery<Profile>("select * from Whiz where account = ?")).execute(result.get(0).getRid());

		Assert.assertEquals(indexedResult.size(), 5);

		ODocument whiz = new ODocument(database.getUnderlying(), "Whiz");
		whiz.field("id", 100);
		whiz.field("text", "This is a test!");
		whiz.field("account", new ODocument(database.getUnderlying(), "Company").field("id", 9999));
		whiz.save();

		Assert.assertTrue(((ODocument) whiz.field("account")).getIdentity().isValid());

		((ODocument) whiz.field("account")).delete();
		whiz.delete();
	}
}
