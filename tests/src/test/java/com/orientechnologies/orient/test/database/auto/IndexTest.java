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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.client.remote.OStorageRemoteThread;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.OClusterPosition;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexException;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OSimpleKeyIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.database.base.OrientTest;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.whiz.Profile;

@Test(groups = { "index" })
public class IndexTest {
  private OObjectDatabaseTx database;
  protected long            startRecordNumber;

  @BeforeMethod
  public void beforeMethod() {
    database.open("admin", "admin");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database.getEntityManager().registerEntityClasses("com.orientechnologies.orient.test.domain.base");
  }

  @AfterMethod
  public void afterMethod() {
    database.close();
  }

  @Parameters(value = "url")
  public IndexTest(String iURL) {
    database = new OObjectDatabaseTx(iURL);
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

    } catch (ORecordDuplicatedException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInUniqueIndex() {
    final OProperty nickProperty = database.getMetadata().getSchema().getClass("Profile").getProperty("nick");
    Assert.assertEquals(nickProperty.getIndexes().iterator().next().getType(), OClass.INDEX_TYPE.UNIQUE.toString());

    final boolean localStorage = !(database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread);

    boolean oldRecording = true;
    long indexQueries = 0L;
    if (localStorage) {
      oldRecording = Orient.instance().getProfiler().isRecording();

      if (!oldRecording) {
        Orient.instance().getProfiler().startRecording();
      }

      indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
      if (indexQueries < 0) {
        indexQueries = 0;
      }
    }

    final List<Profile> result = database.command(
        new OSQLSynchQuery<Profile>(
            "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0' ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']"))
        .execute();

    final List<String> expectedSurnames = new ArrayList<String>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

    if (localStorage && !oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 3);
    for (final Profile profile : result) {
      expectedSurnames.remove(profile.getSurname());
    }

    Assert.assertEquals(expectedSurnames.size(), 0);

    if (localStorage) {
      final long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
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
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
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

    final List<OClusterPosition> positions = getValidPositions(3);

    database.command(new OCommandSQL("insert into index:IDX (key,rid) values (10,#3:" + positions.get(0) + ')')).execute();
    database.command(new OCommandSQL("insert into index:IDX (key,rid) values (20,#3:" + positions.get(1) + ')')).execute();

    List<ODocument> result = database.command(new OCommandSQL("select from index:IDX")).execute();
    Assert.assertNotNull(result);
    Assert.assertEquals(result.size(), 2);
    for (ODocument d : result) {
      Assert.assertTrue(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));

      if (d.field("key").equals(10))
        Assert.assertEquals(d.rawField("rid"), new ORecordId(3, positions.get(0)));
      else if (d.field("key").equals(20))
        Assert.assertEquals(d.rawField("rid"), new ORecordId(3, positions.get(1)));
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
        Assert.assertEquals(d.rawField("rid"), new ORecordId(3, positions.get(0)));
      else if (d.field("key").equals(20))
        Assert.assertEquals(d.rawField("rid"), new ORecordId(3, positions.get(1)));
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
      database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
      database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(OClass.INDEX_TYPE.UNIQUE);
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
      valuesMajorResults.remove(identifiable.getIdentity().getClusterPosition().longValue());
    }
    Assert.assertEquals(valuesMajorResults.size(), 0);

    final Collection<Long> valuesMajorInclusiveResults = new ArrayList<Long>(Arrays.asList(3L, 4L, 5L));
    indexCollection = index.getValuesMajor(3, true);
    Assert.assertEquals(indexCollection.size(), 3);
    for (OIdentifiable identifiable : indexCollection) {
      valuesMajorInclusiveResults.remove(identifiable.getIdentity().getClusterPosition().longValue());
    }
    Assert.assertEquals(valuesMajorInclusiveResults.size(), 0);

    indexCollection = index.getValuesMajor(5, true);
    Assert.assertEquals(indexCollection.size(), 1);
    Assert.assertEquals(indexCollection.iterator().next().getIdentity().getClusterPosition(),
        OClusterPositionFactory.INSTANCE.valueOf(5));

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
      Assert.assertEquals(doc.<ORecordId> rawField("rid"),
          new ORecordId(10, OClusterPositionFactory.INSTANCE.valueOf(doc.<Integer> field("key").longValue())));
    }
    Assert.assertEquals(valuesMajorResults.size(), 0);

    final Collection<Integer> valuesMajorInclusiveResults = new ArrayList<Integer>(Arrays.asList(3, 4, 5));
    indexCollection = index.getEntriesMajor(3, true);
    Assert.assertEquals(indexCollection.size(), 3);
    for (ODocument doc : indexCollection) {
      valuesMajorInclusiveResults.remove(doc.<Integer> field("key"));
      Assert.assertEquals(doc.<ORecordId> rawField("rid"),
          new ORecordId(10, OClusterPositionFactory.INSTANCE.valueOf(doc.<Integer> field("key").longValue())));
    }
    Assert.assertEquals(valuesMajorInclusiveResults.size(), 0);

    indexCollection = index.getEntriesMajor(5, true);
    Assert.assertEquals(indexCollection.size(), 1);
    Assert.assertEquals(indexCollection.iterator().next().<Integer> field("key"), Integer.valueOf(5));
    Assert.assertEquals(indexCollection.iterator().next().<ORecordId> rawField("rid"), new ORecordId(10,
        OClusterPositionFactory.INSTANCE.valueOf(5)));

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
      valuesMinorResults.remove(identifiable.getIdentity().getClusterPosition().longValue());
    }
    Assert.assertEquals(valuesMinorResults.size(), 0);

    final Collection<Long> valuesMinorInclusiveResults = new ArrayList<Long>(Arrays.asList(0L, 1L, 2L, 3L));
    indexCollection = index.getValuesMinor(3, true);
    Assert.assertEquals(indexCollection.size(), 4);
    for (OIdentifiable identifiable : indexCollection) {
      valuesMinorInclusiveResults.remove(identifiable.getIdentity().getClusterPosition().longValue());
    }
    Assert.assertEquals(valuesMinorInclusiveResults.size(), 0);

    indexCollection = index.getValuesMinor(0, true);
    Assert.assertEquals(indexCollection.size(), 1);
    Assert.assertEquals(indexCollection.iterator().next().getIdentity().getClusterPosition(),
        OClusterPositionFactory.INSTANCE.valueOf(0));

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
      Assert.assertEquals(doc.<ORecordId> rawField("rid"),
          new ORecordId(10, OClusterPositionFactory.INSTANCE.valueOf(doc.<Integer> field("key").longValue())));
    }
    Assert.assertEquals(valuesMinorResults.size(), 0);

    final Collection<Integer> valuesMinorInclusiveResults = new ArrayList<Integer>(Arrays.asList(0, 1, 2, 3));
    indexCollection = index.getEntriesMinor(3, true);
    Assert.assertEquals(indexCollection.size(), 4);
    for (ODocument doc : indexCollection) {
      valuesMinorInclusiveResults.remove(doc.<Integer> field("key"));
      Assert.assertEquals(doc.<ORecordId> rawField("rid"),
          new ORecordId(10, OClusterPositionFactory.INSTANCE.valueOf(doc.<Integer> field("key").longValue())));
    }
    Assert.assertEquals(valuesMinorInclusiveResults.size(), 0);

    indexCollection = index.getEntriesMinor(0, true);
    Assert.assertEquals(indexCollection.size(), 1);
    Assert.assertEquals(indexCollection.iterator().next().<Integer> field("key"), Integer.valueOf(0));
    Assert.assertEquals(indexCollection.iterator().next().<ORecordId> rawField("rid"), new ORecordId(10,
        OClusterPositionFactory.INSTANCE.valueOf(0)));

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
      Assert.assertEquals(doc.<ORecordId> rawField("rid"),
          new ORecordId(10, OClusterPositionFactory.INSTANCE.valueOf(doc.<Integer> field("key").longValue())));
    }
    Assert.assertEquals(betweenResults.size(), 0);

    database.command(new OCommandSQL("drop index equalityIdx")).execute();
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorSelect() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    if (indexQueries < 0) {
      indexQueries = 0;
    }

    final List<Profile> result = database.command(
        new OSQLSynchQuery<Profile>("select * from Profile where nick > 'ZZZJayLongNickIndex3'")).execute();
    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 2);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    Assert.assertEquals(newIndexQueries, indexQueries + 1);
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorEqualsSelect() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    if (indexQueries < 0) {
      indexQueries = 0;
    }

    final List<Profile> result = database.command(
        new OSQLSynchQuery<Profile>("select * from Profile where nick >= 'ZZZJayLongNickIndex3'")).execute();
    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4",
        "ZZZJayLongNickIndex5"));

    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 3);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    Assert.assertEquals(newIndexQueries, indexQueries + 1);
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMinorSelect() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    if (indexQueries < 0) {
      indexQueries = 0;
    }

    final List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick < '002'"))
        .execute();
    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("000", "001"));

    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 2);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    Assert.assertEquals(newIndexQueries, indexQueries + 1);
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMinorEqualsSelect() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    if (indexQueries < 0) {
      indexQueries = 0;
    }

    final List<Profile> result = database.command(new OSQLSynchQuery<Profile>("select * from Profile where nick <= '002'"))
        .execute();
    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("000", "001", "002"));

    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 3);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    Assert.assertEquals(newIndexQueries, indexQueries + 1);
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexBetweenSelect() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    if (indexQueries < 0) {
      indexQueries = 0;
    }

    final List<Profile> result = database.command(
        new OSQLSynchQuery<Profile>("select * from Profile where nick between '001' and '004'")).execute();
    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("001", "002", "003", "004"));

    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 4);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    Assert.assertEquals(newIndexQueries, indexQueries + 1);
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInComplexSelectOne() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    if (indexQueries < 0) {
      indexQueries = 0;
    }

    final List<Profile> result = database.command(
        new OSQLSynchQuery<Profile>("select * from Profile where (name = 'Giuseppe' OR name <> 'Napoleone')"
            + " AND (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND (nick >= 'ZZZJayLongNickIndex3'))"))
        .execute();
    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4",
        "ZZZJayLongNickIndex5"));
    Assert.assertEquals(result.size(), 3);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
    Assert.assertEquals(newIndexQueries, indexQueries + 1);
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInComplexSelectTwo() {
    if (database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread) {
      return;
    }

    final boolean oldRecording = Orient.instance().getProfiler().isRecording();

    if (!oldRecording) {
      Orient.instance().getProfiler().startRecording();
    }

    long indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
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
      Orient.instance().getProfiler().stopRecording();
    }

    final List<String> expectedNicks = new ArrayList<String>(Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4",
        "ZZZJayLongNickIndex5"));
    Assert.assertEquals(result.size(), 3);
    for (Profile profile : result) {
      expectedNicks.remove(profile.getNick());
    }

    Assert.assertEquals(expectedNicks.size(), 0);
    long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
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
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
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
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    database.getMetadata().getSchema().save();
  }

  @Test(dependsOnMethods = { "createNotUniqueIndexOnNick", "populateIndexDocuments" })
  public void testIndexInNotUniqueIndex() {
    final OProperty nickProperty = database.getMetadata().getSchema().getClass("Profile").getProperty("nick");
    Assert.assertEquals(nickProperty.getIndexes().iterator().next().getType(), OClass.INDEX_TYPE.NOTUNIQUE.toString());

    final boolean localStorage = !(database.getStorage() instanceof OStorageRemote || database.getStorage() instanceof OStorageRemoteThread);

    boolean oldRecording = true;
    long indexQueries = 0L;
    if (localStorage) {
      oldRecording = Orient.instance().getProfiler().isRecording();

      if (!oldRecording) {
        Orient.instance().getProfiler().startRecording();
      }

      indexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
      if (indexQueries < 0) {
        indexQueries = 0;
      }
    }

    final List<Profile> result = database.command(
        new OSQLSynchQuery<Profile>(
            "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0' ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']"))
        .execute();

    final List<String> expectedSurnames = new ArrayList<String>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

    if (localStorage && !oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 3);
    for (final Profile profile : result) {
      expectedSurnames.remove(profile.getSurname());
    }

    Assert.assertEquals(expectedSurnames.size(), 0);

    if (localStorage) {
      final long newIndexQueries = Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
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
      multiGetResults.remove(identifiable.getIdentity().getClusterPosition().intValue());
    }
    Assert.assertEquals(multiGetResults.size(), 0);

    database.command(new OCommandSQL("drop index inIdx")).execute();
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexGetValuesNotUniqueIndex() {
    database.command(new OCommandSQL("create index inIdx notunique")).execute();

    database.getMetadata().getIndexManager().reload();
    Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("inIdx"));

    for (int i = 0; i < 2; i++)
      for (int key = 0; key <= 2; key++) {
        database.command(new OCommandSQL("insert into index:inIdx (key,rid) values (" + key + ",#10:" + (i + key * 2) + ")"))
            .execute();
      }

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("inIdx");
    final Collection<Integer> multiGetResults = new ArrayList<Integer>(Arrays.asList(0, 1, 4, 5));
    final Collection<OIdentifiable> indexCollection = index.getValues(Arrays.asList(0, 2));
    Assert.assertEquals(indexCollection.size(), 4);
    for (final OIdentifiable identifiable : indexCollection) {
      multiGetResults.remove(identifiable.getIdentity().getClusterPosition().intValue());
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
      multiGetResults.remove(doc.<Integer> field("key"));
    }
    Assert.assertEquals(multiGetResults.size(), 0);

    database.command(new OCommandSQL("drop index inIdx")).execute();
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexGetEntriesNotUniqueIndex() {
    database.command(new OCommandSQL("create index inIdx notunique")).execute();

    database.getMetadata().getIndexManager().reload();
    Assert.assertNotNull(database.getMetadata().getIndexManager().getIndex("inIdx"));

    for (int i = 0; i < 2; i++)
      for (int key = 0; key <= 2; key++) {
        database.command(new OCommandSQL("insert into index:inIdx (key,rid) values (" + key + ",#10:" + (i + key * 2) + ")"))
            .execute();
      }

    final OIndex<?> index = database.getMetadata().getIndexManager().getIndex("inIdx");
    final Collection<Integer> multiGetResults = new ArrayList<Integer>(Arrays.asList(0, 0, 2, 2));
    final Collection<ODocument> indexCollection = index.getEntries(Arrays.asList(0, 2));
    Assert.assertEquals(indexCollection.size(), 4);
    for (final ODocument doc : indexCollection) {
      multiGetResults.remove(doc.<Integer> field("key"));
    }
    Assert.assertEquals(multiGetResults.size(), 0);

    database.command(new OCommandSQL("drop index inIdx")).execute();
  }

  @Test
  public void testIndexCount() {
    final OIndex<?> nickIndex = database.getMetadata().getIndexManager().getIndex("Profile.nick");
    final List<ODocument> result = database.query(new OSQLSynchQuery<Object>("select count(*) from index:Profile.nick"));
    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(result.get(0).<Long> field("count").longValue(), nickIndex.getSize());
  }

  @SuppressWarnings("unchecked")
  public void longTypes() {
    database.getMetadata().getSchema().getClass("Profile").createProperty("hash", OType.LONG).createIndex(OClass.INDEX_TYPE.UNIQUE);

    OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) database.getMetadata().getIndexManager().getIndex("Profile.hash");

    for (int i = 0; i < 5; i++) {
      Profile profile = new Profile("HashTest" + i).setHash(100l + i);
      database.save(profile);
    }

    Iterator<Entry<Object, OIdentifiable>> it = idx.iterator();
    while (it.hasNext()) {
      it.next();
    }

    Assert.assertEquals(idx.getSize(), 5);
  }

  public void indexLinks() {
    database.getMetadata().getSchema().getClass("Whiz").getProperty("account").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    final List<Account> result = database.command(new OSQLSynchQuery<Account>("select * from Account limit 1")).execute();

    final OIndex<?> idx = database.getMetadata().getIndexManager().getIndex("Whiz.account");

    for (int i = 0; i < 5; i++) {
      final ODocument whiz = new ODocument("Whiz");

      whiz.field("id", i);
      whiz.field("text", "This is a test");
      whiz.field("account", result.get(0).getRid());

      whiz.save();
    }

    Assert.assertEquals(idx.getSize(), 5);

    final List<ODocument> indexedResult = database.getUnderlying()
        .command(new OSQLSynchQuery<Profile>("select * from Whiz where account = ?")).execute(result.get(0).getRid());

    Assert.assertEquals(indexedResult.size(), 5);

    for (final ODocument resDoc : indexedResult) {
      resDoc.delete();
    }

    final ODocument whiz = new ODocument("Whiz");
    whiz.field("id", 100);
    whiz.field("text", "This is a test!");
    whiz.field("account", new ODocument("Company").field("id", 9999));
    whiz.save();

    Assert.assertTrue(((ODocument) whiz.field("account")).getIdentity().isValid());

    ((ODocument) whiz.field("account")).delete();
    whiz.delete();
  }

  public void linkedIndexedProperty() {
    ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TestClass")) {
      OClass testClass = db.getMetadata().getSchema().createClass("TestClass");
      OClass testLinkClass = db.getMetadata().getSchema().createClass("TestLinkClass");
      testClass.createProperty("testLink", OType.LINK, testLinkClass).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      testClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
      testLinkClass.createProperty("testBoolean", OType.BOOLEAN);
      testLinkClass.createProperty("testString", OType.STRING);
      db.getMetadata().getSchema().save();
    }
    ODocument testClassDocument = db.newInstance("TestClass");
    testClassDocument.field("name", "Test Class 1");
    ODocument testLinkClassDocument = new ODocument("TestLinkClass");
    testLinkClassDocument.field("testString", "Test Link Class 1");
    testLinkClassDocument.field("testBoolean", true);
    testClassDocument.field("testLink", testLinkClassDocument);
    testClassDocument.save();
    // THIS WILL THROW A java.lang.ClassCastException: com.orientechnologies.orient.core.id.ORecordId cannot be cast to
    // java.lang.Boolean
    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from TestClass where testLink.testBoolean = true"));
    Assert.assertEquals(result.size(), 1);
    // THIS WILL THROW A java.lang.ClassCastException: com.orientechnologies.orient.core.id.ORecordId cannot be cast to
    // java.lang.String
    result = db.query(new OSQLSynchQuery<ODocument>("select from TestClass where testLink.testString = 'Test Link Class 1'"));
    Assert.assertEquals(result.size(), 1);

    db.close();
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testLinkedIndexedPropertyInTx() {
    ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    db.begin();
    ODocument testClassDocument = db.newInstance("TestClass");
    testClassDocument.field("name", "Test Class 2");
    ODocument testLinkClassDocument = new ODocument("TestLinkClass");
    testLinkClassDocument.field("testString", "Test Link Class 2");
    testLinkClassDocument.field("testBoolean", true);
    testClassDocument.field("testLink", testLinkClassDocument);
    testClassDocument.save();
    db.commit();

    // THIS WILL THROW A java.lang.ClassCastException: com.orientechnologies.orient.core.id.ORecordId cannot be cast to
    // java.lang.Boolean
    List<ODocument> result = db.query(new OSQLSynchQuery<ODocument>("select from TestClass where testLink.testBoolean = true"));
    Assert.assertEquals(result.size(), 2);
    // THIS WILL THROW A java.lang.ClassCastException: com.orientechnologies.orient.core.id.ORecordId cannot be cast to
    // java.lang.String
    result = db.query(new OSQLSynchQuery<ODocument>("select from TestClass where testLink.testString = 'Test Link Class 2'"));
    Assert.assertEquals(result.size(), 1);

    db.close();
  }

  public void testDictionary() {
    ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    OClass pClass = db.getMetadata().getSchema().createClass("Person2");
    pClass.createProperty("firstName", OType.STRING);
    pClass.createProperty("lastName", OType.STRING);
    pClass.createProperty("age", OType.INTEGER);
    pClass.createIndex("testIdx", INDEX_TYPE.DICTIONARY, "firstName", "lastName");

    ODocument person = new ODocument("Person2");
    person.field("firstName", "foo").field("lastName", "bar").save();

    person = new ODocument("Person2");
    person.field("firstName", "foo").field("lastName", "bar").field("age", 32).save();

    db.close();
  }

  public void testConcurrentRemoveDelete() {
    ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("MyFruit")) {
      OClass fruitClass = db.getMetadata().getSchema().createClass("MyFruit");
      fruitClass.createProperty("name", OType.STRING);
      fruitClass.createProperty("color", OType.STRING);

      db.getMetadata().getSchema().getClass("MyFruit").getProperty("name").createIndex(OClass.INDEX_TYPE.UNIQUE);

      db.getMetadata().getSchema().getClass("MyFruit").getProperty("color").createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

      db.getMetadata().getSchema().save();
    }

    long expectedIndexSize = 0;

    final int passCount = 10;
    final int chunkSize = 1000;
    for (int pass = 0; pass < passCount; pass++) {
      List<ODocument> recordsToDelete = new ArrayList<ODocument>();
      db.begin();
      for (int i = 0; i < chunkSize; i++) {
        ODocument d = new ODocument("MyFruit").field("name", "ABC" + pass + 'K' + i).field("color", "FOO" + pass);
        d.save();
        if (i < chunkSize / 2) {
          recordsToDelete.add(d);
        }
      }
      db.commit();

      expectedIndexSize += chunkSize;
      Assert.assertEquals(db.getMetadata().getIndexManager().getClassIndex("MyFruit", "MyFruit.color").getSize(),
          expectedIndexSize, "After add");

      // do delete
      db.begin();
      for (final ODocument recordToDelete : recordsToDelete) {
        Assert.assertNotNull(db.delete(recordToDelete));
      }
      db.commit();

      expectedIndexSize -= recordsToDelete.size();
      Assert.assertEquals(db.getMetadata().getIndexManager().getClassIndex("MyFruit", "MyFruit.color").getSize(),
          expectedIndexSize, "After delete");
    }

    db.close();
  }

  public void testIndexParamsAutoConversion() {
    ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("IndexTestTerm")) {
      final OClass termClass = db.getMetadata().getSchema().createClass("IndexTestTerm");
      termClass.createProperty("label", OType.STRING);
      termClass.createIndex("idxTerm", INDEX_TYPE.UNIQUE, "label");

      db.getMetadata().getSchema().save();
    }

    final ODocument doc = new ODocument("IndexTestTerm");
    doc.field("label", "42");
    doc.save();

    final ORecordId result = (ORecordId) db.getMetadata().getIndexManager().getIndex("idxTerm").get("42");
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getIdentity(), doc.getIdentity());
  }

  public void testTransactionUniqueIndexTestOne() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final OClass termClass = db.getMetadata().getSchema().createClass("TransactionUniqueIndexTest");
      termClass.createProperty("label", OType.STRING);
      termClass.createIndex("idxTransactionUniqueIndexTest", INDEX_TYPE.UNIQUE, "label");
      db.getMetadata().getSchema().save();
    }

    ODocument docOne = new ODocument("TransactionUniqueIndexTest");
    docOne.field("label", "A");
    docOne.save();

    final List<ODocument> resultBeforeCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from index:idxTransactionUniqueIndexTest"));
    Assert.assertEquals(resultBeforeCommit.size(), 1);

    db.begin();
    try {
      ODocument docTwo = new ODocument("TransactionUniqueIndexTest");
      docTwo.field("label", "A");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException oie) {
    }

    final List<ODocument> resultAfterCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from index:idxTransactionUniqueIndexTest"));
    Assert.assertEquals(resultAfterCommit.size(), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestOne")
  public void testTransactionUniqueIndexTestTwo() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final OClass termClass = db.getMetadata().getSchema().createClass("TransactionUniqueIndexTest");
      termClass.createProperty("label", OType.STRING);
      termClass.createIndex("idxTransactionUniqueIndexTest", INDEX_TYPE.UNIQUE, "label");
      db.getMetadata().getSchema().save();
    }

    final List<ODocument> resultBeforeCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from index:idxTransactionUniqueIndexTest"));
    Assert.assertEquals(resultBeforeCommit.size(), 1);

    db.begin();

    try {
      ODocument docOne = new ODocument("TransactionUniqueIndexTest");
      docOne.field("label", "B");
      docOne.save();

      ODocument docTwo = new ODocument("TransactionUniqueIndexTest");
      docTwo.field("label", "B");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException oie) {
    }

    final List<ODocument> resultAfterCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from index:idxTransactionUniqueIndexTest"));
    Assert.assertEquals(resultAfterCommit.size(), 1);
  }

  public void testTransactionUniqueIndexTestWithDotNameOne() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final OClass termClass = db.getMetadata().getSchema().createClass("TransactionUniqueIndexWithDotTest");
      termClass.createProperty("label", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
      db.getMetadata().getSchema().save();
    }

    ODocument docOne = new ODocument("TransactionUniqueIndexWithDotTest");
    docOne.field("label", "A");
    docOne.save();

    final List<ODocument> resultBeforeCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from  index:TransactionUniqueIndexWithDotTest.label"));
    Assert.assertEquals(resultBeforeCommit.size(), 1);

    long countClassBefore = db.countClass("TransactionUniqueIndexWithDotTest");
    db.begin();
    try {
      ODocument docTwo = new ODocument("TransactionUniqueIndexWithDotTest");
      docTwo.field("label", "A");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException oie) {
    }

    Assert.assertEquals(
        ((List<ODocument>) db.command(new OCommandSQL("select from TransactionUniqueIndexWithDotTest")).execute()).size(),
        countClassBefore);

    final List<ODocument> resultAfterCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from  index:TransactionUniqueIndexWithDotTest.label"));
    Assert.assertEquals(resultAfterCommit.size(), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestWithDotNameOne")
  public void testTransactionUniqueIndexTestWithDotNameTwo() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final OClass termClass = db.getMetadata().getSchema().createClass("TransactionUniqueIndexWithDotTest");
      termClass.createProperty("label", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
      db.getMetadata().getSchema().save();
    }

    final List<ODocument> resultBeforeCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from index:TransactionUniqueIndexWithDotTest.label"));
    Assert.assertEquals(resultBeforeCommit.size(), 1);

    db.begin();

    try {
      ODocument docOne = new ODocument("TransactionUniqueIndexWithDotTest");
      docOne.field("label", "B");
      docOne.save();

      ODocument docTwo = new ODocument("TransactionUniqueIndexWithDotTest");
      docTwo.field("label", "B");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException oie) {
    }

    final List<ODocument> resultAfterCommit = db.query(new OSQLSynchQuery<ODocument>(
        "select from  index:TransactionUniqueIndexWithDotTest.label"));
    Assert.assertEquals(resultAfterCommit.size(), 1);
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testIndexRemoval() {
    List<ODocument> result = database.command(new OCommandSQL("select rid from index:Profile.nick")).execute();
    Assert.assertNotNull(result);

    ODocument firstProfile = null;

    for (ODocument d : result) {
      if (firstProfile == null)
        firstProfile = d.field("rid");

      Assert.assertFalse(d.containsField("key"));
      Assert.assertTrue(d.containsField("rid"));
    }

    result = database.command(new OCommandSQL("select rid from index:Profile.nick where key = ?")).execute(
        firstProfile.field("nick"));

    Assert.assertNotNull(result);
    Assert.assertEquals(result.get(0).field("rid", OType.LINK), firstProfile.getIdentity());

    firstProfile.delete();

    result = database.command(new OCommandSQL("select rid from index:Profile.nick where key = ?")).execute(
        firstProfile.field("nick"));
    Assert.assertTrue(result.isEmpty());

  }

  public void createInheritanceIndex() {
    ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL());
    try {
      db.open("admin", "admin");

      if (!db.getMetadata().getSchema().existsClass("BaseTestClass")) {
        OClass baseClass = db.getMetadata().getSchema().createClass("BaseTestClass");
        OClass childClass = db.getMetadata().getSchema().createClass("ChildTestClass");
        OClass anotherChildClass = db.getMetadata().getSchema().createClass("AnotherChildTestClass");

        if (!baseClass.isSuperClassOf(childClass))
          childClass.setSuperClass(baseClass);
        if (!baseClass.isSuperClassOf(anotherChildClass))
          anotherChildClass.setSuperClass(baseClass);

        baseClass.createProperty("testParentProperty", OType.LONG).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

        db.getMetadata().getSchema().save();
      }

      ODocument childClassDocument = db.newInstance("ChildTestClass");
      childClassDocument.field("testParentProperty", 10L);
      childClassDocument.save();

      ODocument anotherChildClassDocument = db.newInstance("AnotherChildTestClass");
      anotherChildClassDocument.field("testParentProperty", 11L);
      anotherChildClassDocument.save();

      Assert.assertFalse(new ORecordId(-1, ORecordId.CLUSTER_POS_INVALID).equals(childClassDocument.getIdentity()));
      Assert.assertFalse(new ORecordId(-1, ORecordId.CLUSTER_POS_INVALID).equals(anotherChildClassDocument.getIdentity()));
    } finally {
      db.close();
    }
  }

  @Test(dependsOnMethods = "createInheritanceIndex")
  public void testIndexReturnOnlySpecifiedClass() throws Exception {
    List<ODocument> result;

    ODatabaseDocument db = database.getUnderlying();

    result = db.command(new OSQLSynchQuery("select * from ChildTestClass where testParentProperty = 10")).execute();
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(10L, result.get(0).field("testParentProperty"));

    result = db.command(new OCommandSQL("select * from AnotherChildTestClass where testParentProperty = 11")).execute();
    Assert.assertNotNull(result);
    Assert.assertEquals(1, result.size());
    Assert.assertEquals(11L, result.get(0).field("testParentProperty"));
  }

  @Test
  public void testManualIndexInTx() {
    if (database.getURL().startsWith("remote:"))
      return;

    ODatabaseDocumentTx db = (ODatabaseDocumentTx) database.getUnderlying();

    database.getMetadata().getSchema().createClass("ManualIndexTxClass");

    OIndexManager idxManager = db.getMetadata().getIndexManager();
    idxManager.createIndex("manualTxIndexTest", "UNIQUE", new OSimpleKeyIndexDefinition(OType.INTEGER), null, null);
    OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) idxManager.getIndex("manualTxIndexTest");

    ODocument v0 = new ODocument("ManualIndexTxClass");
    v0.field("counter", 0);
    v0.save();
    idx.put(0, v0);
    Assert.assertTrue(idx.contains(0));

    db.begin(OTransaction.TXTYPE.OPTIMISTIC);
    ODocument v = new ODocument("ManualIndexTxClass");
    v.field("counter", 52);
    v.save();

    ODocument v2 = new ODocument("ManualIndexTxClass");
    v2.field("counter", 54);
    v2.save();

    Assert.assertNotNull(idx);
    idx.remove(0);
    idx.put(52, v);

    db.commit();

    Assert.assertTrue(idx.contains(52));
    Assert.assertFalse(idx.contains(0));
    Assert.assertTrue(idx.get(52).getIdentity().isPersistent());
    Assert.assertEquals(idx.get(52).getIdentity(), v.getIdentity());
  }

  @Test
  public void testManualIndexInTxRecursiveStore() {
    if (database.getURL().startsWith("remote:"))
      return;

    ODatabaseDocumentTx db = (ODatabaseDocumentTx) database.getUnderlying();

    database.getMetadata().getSchema().createClass("ManualIndexTxRecursiveStoreClass");

    OIndexManager idxManager = db.getMetadata().getIndexManager();
    idxManager.createIndex("manualTxIndexRecursiveStoreTest", "UNIQUE", new OSimpleKeyIndexDefinition(OType.INTEGER), null, null);

    OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) idxManager.getIndex("manualTxIndexRecursiveStoreTest");

    ODocument v0 = new ODocument("ManualIndexTxRecursiveStoreClass");
    v0.field("counter", 0);
    v0.save();
    idx.put(0, v0);
    Assert.assertTrue(idx.contains(0));

    db.begin(OTransaction.TXTYPE.OPTIMISTIC);
    ODocument v = new ODocument("ManualIndexTxRecursiveStoreClass");
    v.field("counter", 52);

    ODocument v2 = new ODocument("ManualIndexTxRecursiveStoreClass");
    v2.field("counter", 54);
    v2.field("link", v);
    v2.save();

    v.field("link", v2);
    v.save();

    Assert.assertNotNull(idx);
    idx.remove(0);

    idx.put(52, v);
    idx.put(54, v2);

    db.commit();

    Assert.assertTrue(idx.contains(52));
    Assert.assertTrue(idx.contains(54));

    Assert.assertFalse(idx.contains(0));

    Assert.assertTrue(idx.get(52).getIdentity().isPersistent());
    Assert.assertEquals(idx.get(52).getIdentity(), v.getIdentity());

    Assert.assertTrue(idx.get(54).getIdentity().isPersistent());
    Assert.assertEquals(idx.get(54).getIdentity(), v2.getIdentity());
  }

  public void testIndexCountPlusCondition() {
    OIndexManager idxManager = database.getMetadata().getIndexManager();
    idxManager.createIndex("IndexCountPlusCondition", "NOTUNIQUE", new OSimpleKeyIndexDefinition(OType.INTEGER), null, null);

    final OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) idxManager.getIndex("IndexCountPlusCondition");

    final Map<Integer, Long> keyDocsCount = new HashMap<Integer, Long>();
    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      final ODocument doc = new ODocument();
      doc.save();

      idx.put(key, doc);

      if (keyDocsCount.containsKey(key))
        keyDocsCount.put(key, keyDocsCount.get(key) + 1);
      else
        keyDocsCount.put(key, 1L);
    }

    for (Map.Entry<Integer, Long> entry : keyDocsCount.entrySet()) {
      List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
          "select count(*) from index:IndexCountPlusCondition where key = ?"), entry.getKey());
      Assert.assertEquals(result.get(0).<Long> field("count"), entry.getValue());
    }
  }

  public void testNotUniqueIndexKeySize() {
    OIndexManager idxManager = database.getMetadata().getIndexManager();
    idxManager.createIndex("IndexNotUniqueIndexKeySize", "NOTUNIQUE", new OSimpleKeyIndexDefinition(OType.INTEGER), null, null);

    final OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) idxManager.getIndex("IndexNotUniqueIndexKeySize");

    final Set<Integer> keys = new HashSet<Integer>();
    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      final ODocument doc = new ODocument();
      doc.save();

      idx.put(key, doc);

      keys.add(key);
    }

    Assert.assertEquals(idx.getKeySize(), keys.size());
  }

  public void testNotUniqueIndexSize() {
    OIndexManager idxManager = database.getMetadata().getIndexManager();
    idxManager.createIndex("IndexNotUniqueIndexSize", "NOTUNIQUE", new OSimpleKeyIndexDefinition(OType.INTEGER), null, null);

    final OIndex<OIdentifiable> idx = (OIndex<OIdentifiable>) idxManager.getIndex("IndexNotUniqueIndexSize");

    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      final ODocument doc = new ODocument();
      doc.save();

      idx.put(key, doc);
    }

    Assert.assertEquals(idx.getSize(), 99);
  }

  @Test
  public void testIndexRebuildDuringNonProxiedObjectDelete() {
    Profile profile = new Profile("NonProxiedObjectToDelete", "NonProxiedObjectToDelete", "NonProxiedObjectToDelete", null);
    profile = database.save(profile);

    OIndexManager idxManager = database.getMetadata().getIndexManager();
    OIndex<?> nickIndex = idxManager.getIndex("Profile.nick");

    Assert.assertTrue(nickIndex.contains("NonProxiedObjectToDelete"));

    final Profile loadedProfile = database.load(new ORecordId(profile.getId()));
    database.delete(database.detach(loadedProfile, true));

    Assert.assertFalse(nickIndex.contains("NonProxiedObjectToDelete"));
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringNonProxiedObjectDelete")
  public void testIndexRebuildDuringDetachAllNonProxiedObjectDelete() {
    Profile profile = new Profile("NonProxiedObjectToDelete", "NonProxiedObjectToDelete", "NonProxiedObjectToDelete", null);
    profile = database.save(profile);

    OIndexManager idxManager = database.getMetadata().getIndexManager();
    OIndex<?> nickIndex = idxManager.getIndex("Profile.nick");

    Assert.assertTrue(nickIndex.contains("NonProxiedObjectToDelete"));

    final Profile loadedProfile = database.load(new ORecordId(profile.getId()));
    database.delete(database.detachAll(loadedProfile, true));

    Assert.assertFalse(nickIndex.contains("NonProxiedObjectToDelete"));
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringDetachAllNonProxiedObjectDelete")
  public void testRestoreUniqueIndex() {
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").createIndex(OClass.INDEX_TYPE.UNIQUE);
  }

  private List<OClusterPosition> getValidPositions(int clusterId) {
    final List<OClusterPosition> positions = new ArrayList<OClusterPosition>();

    final ORecordIteratorCluster<?> iteratorCluster = database.getUnderlying()
        .browseCluster(database.getClusterNameById(clusterId));

    for (int i = 0; i < 7; i++) {
      if (!iteratorCluster.hasNext())
        break;

      ORecord<?> doc = iteratorCluster.next();
      positions.add(doc.getIdentity().getClusterPosition());
    }
    return positions;
  }
}
