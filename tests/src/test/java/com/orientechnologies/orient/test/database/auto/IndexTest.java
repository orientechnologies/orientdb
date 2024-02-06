/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManagerAbstract;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.core.storage.cache.OWriteCache;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.test.database.base.OrientTest;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.whiz.Profile;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@SuppressWarnings({"deprecation", "unchecked"})
@Test(groups = {"index"})
public class IndexTest extends ObjectDBBaseTest {
  @Parameters(value = "url")
  public IndexTest(@Optional String url) {
    super(url);
  }

  @BeforeClass
  public void beforeClass() throws Exception {
    super.beforeClass();

    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.business");
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.whiz");
    database
        .getEntityManager()
        .registerEntityClasses("com.orientechnologies.orient.test.domain.base");
  }

  public void testDuplicatedIndexOnUnique() {
    Profile jayMiner = new Profile("Jay", "Jay", "Miner", null);
    database.save(jayMiner);

    Profile jacobMiner = new Profile("Jay", "Jacob", "Miner", null);

    try {
      database.save(jacobMiner);

      // IT SHOULD GIVE ERROR ON DUPLICATED KEY
      Assert.fail();

    } catch (ORecordDuplicatedException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInUniqueIndex() {
    final OProperty nickProperty =
        database.getMetadata().getSchema().getClass("Profile").getProperty("nick");
    Assert.assertEquals(
        nickProperty.getIndexes().iterator().next().getType(), OClass.INDEX_TYPE.UNIQUE.toString());

    final boolean localStorage =
        !(((ODatabaseDocumentInternal) database.getUnderlying()).isRemote());

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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0' ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']"))
            .execute();

    final List<String> expectedSurnames =
        new ArrayList<>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

    if (localStorage && !oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 3);
    for (final Profile profile : result) {
      expectedSurnames.remove(profile.getSurname());
    }

    Assert.assertEquals(expectedSurnames.size(), 0);

    if (localStorage) {
      final long newIndexQueries =
          Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
      Assert.assertEquals(newIndexQueries, indexQueries + 1);
    }
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testUseOfIndex() {
    final List<Profile> result =
        database
            .command(new OSQLSynchQuery<Profile>("select * from Profile where nick = 'Jay'"))
            .execute();

    Assert.assertFalse(result.isEmpty());

    Profile record;
    for (int i = 0; i < result.size(); ++i) {
      record = result.get(i);

      OrientTest.printRecord(i, record);

      Assert.assertTrue(record.getName().equalsIgnoreCase("Jay"));
    }
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testIndexEntries() {
    checkEmbeddedDB();

    List<Profile> result =
        database
            .command(new OSQLSynchQuery<Profile>("select * from Profile where nick is not null"))
            .execute();

    OIndex idx =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex((ODatabaseDocumentInternal) database.getUnderlying(), "Profile.nick");

    Assert.assertEquals(idx.getInternal().size(), result.size());
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnUnique")
  public void testIndexSize() {
    checkEmbeddedDB();

    List<Profile> result =
        database
            .command(new OSQLSynchQuery<Profile>("select * from Profile where nick is not null"))
            .execute();

    int profileSize = result.size();

    database.getMetadata().getIndexManagerInternal().reload();
    Assert.assertEquals(
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex((ODatabaseDocumentInternal) database.getUnderlying(), "Profile.nick")
            .getInternal()
            .size(),
        profileSize);
    for (int i = 0; i < 10; i++) {
      Profile profile = new Profile("Yay-" + i, "Jay", "Miner", null);
      database.save(profile);
      profileSize++;
      try (Stream<ORID> stream =
          database
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex((ODatabaseDocumentInternal) database.getUnderlying(), "Profile.nick")
              .getInternal()
              .getRids("Yay-" + i)) {
        Assert.assertTrue(stream.findAny().isPresent());
      }
    }
  }

  @Test(dependsOnMethods = "testUseOfIndex")
  public void testChangeOfIndexToNotUnique() {
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
  }

  @Test(dependsOnMethods = "testChangeOfIndexToNotUnique")
  public void testDuplicatedIndexOnNotUnique() {
    Profile nickNolte = new Profile("Jay", "Nick", "Nolte", null);
    database.save(nickNolte);
  }

  @Test(dependsOnMethods = "testDuplicatedIndexOnNotUnique")
  public void testChangeOfIndexToUnique() {
    try {
      database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
      database
          .getMetadata()
          .getSchema()
          .getClass("Profile")
          .getProperty("nick")
          .createIndex(OClass.INDEX_TYPE.UNIQUE);
      Assert.fail();
    } catch (ORecordDuplicatedException e) {
      Assert.assertTrue(true);
    }
  }

  @Test(dependsOnMethods = "populateIndexDocuments")
  public void testIndexInMajorSelect() {
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "select * from Profile where nick > 'ZZZJayLongNickIndex3'"))
            .execute();
    final List<String> expectedNicks =
        new ArrayList<>(Arrays.asList("ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

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
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "select * from Profile where nick >= 'ZZZJayLongNickIndex3'"))
            .execute();
    final List<String> expectedNicks =
        new ArrayList<>(
            Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));

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
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(new OSQLSynchQuery<Profile>("select * from Profile where nick < '002'"))
            .execute();
    final List<String> expectedNicks = new ArrayList<>(Arrays.asList("000", "001"));

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
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(new OSQLSynchQuery<Profile>("select * from Profile where nick <= '002'"))
            .execute();
    final List<String> expectedNicks = new ArrayList<>(Arrays.asList("000", "001", "002"));

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
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "select * from Profile where nick between '001' and '004'"))
            .execute();
    final List<String> expectedNicks = new ArrayList<>(Arrays.asList("001", "002", "003", "004"));

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
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "select * from Profile where (name = 'Giuseppe' OR name <> 'Napoleone')"
                        + " AND (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND (nick >= 'ZZZJayLongNickIndex3'))"))
            .execute();
    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    final List<String> expectedNicks =
        new ArrayList<>(
            Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));
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
    if (((ODatabaseDocumentInternal) database.getUnderlying()).isRemote()) {
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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "select * from Profile where "
                        + "((name = 'Giuseppe' OR name <> 'Napoleone')"
                        + " AND (nick is not null AND (name = 'Giuseppe' OR name <> 'Napoleone') AND (nick >= 'ZZZJayLongNickIndex3' OR nick >= 'ZZZJayLongNickIndex4')))"))
            .execute();
    if (!oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    final List<String> expectedNicks =
        new ArrayList<>(
            Arrays.asList("ZZZJayLongNickIndex3", "ZZZJayLongNickIndex4", "ZZZJayLongNickIndex5"));
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
      final Profile profile =
          new Profile("ZZZJayLongNickIndex" + i, "NickIndex" + i, "NolteIndex" + i, null);
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
  }

  @Test(dependsOnMethods = "removeNotUniqueIndexOnNick")
  public void testQueryingWithoutNickIndex() {
    Assert.assertTrue(
        database.getMetadata().getSchema().getClass("Profile").getProperty("name").isIndexed());
    Assert.assertFalse(
        database.getMetadata().getSchema().getClass("Profile").getProperty("nick").isIndexed());

    List<Profile> result =
        database
            .command(new OSQLSynchQuery<ODocument>("SELECT FROM Profile WHERE nick = 'Jay'"))
            .execute();
    Assert.assertEquals(result.size(), 2);

    result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Jay'"))
            .execute();
    Assert.assertEquals(result.size(), 1);

    result =
        database
            .command(
                new OSQLSynchQuery<ODocument>(
                    "SELECT FROM Profile WHERE nick = 'Jay' AND name = 'Nick'"))
            .execute();
    Assert.assertEquals(result.size(), 1);
  }

  @Test(dependsOnMethods = "testQueryingWithoutNickIndex")
  public void createNotUniqueIndexOnNick() {
    database
        .getMetadata()
        .getSchema()
        .getClass("Profile")
        .getProperty("nick")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
  }

  @Test(dependsOnMethods = {"createNotUniqueIndexOnNick", "populateIndexDocuments"})
  public void testIndexInNotUniqueIndex() {
    final OProperty nickProperty =
        database.getMetadata().getSchema().getClass("Profile").getProperty("nick");
    Assert.assertEquals(
        nickProperty.getIndexes().iterator().next().getType(),
        OClass.INDEX_TYPE.NOTUNIQUE.toString());

    final boolean localStorage =
        !(((ODatabaseDocumentInternal) database.getUnderlying()).isRemote());

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

    final List<Profile> result =
        database
            .command(
                new OSQLSynchQuery<Profile>(
                    "SELECT * FROM Profile WHERE nick in ['ZZZJayLongNickIndex0' ,'ZZZJayLongNickIndex1', 'ZZZJayLongNickIndex2']"))
            .execute();

    final List<String> expectedSurnames =
        new ArrayList<>(Arrays.asList("NolteIndex0", "NolteIndex1", "NolteIndex2"));

    if (localStorage && !oldRecording) {
      Orient.instance().getProfiler().stopRecording();
    }

    Assert.assertEquals(result.size(), 3);
    for (final Profile profile : result) {
      expectedSurnames.remove(profile.getSurname());
    }

    Assert.assertEquals(expectedSurnames.size(), 0);

    if (localStorage) {
      final long newIndexQueries =
          Orient.instance().getProfiler().getCounter("db.demo.query.indexUsed");
      Assert.assertEquals(newIndexQueries, indexQueries + 1);
    }
  }

  public void indexLinks() {
    checkEmbeddedDB();

    database
        .getMetadata()
        .getSchema()
        .getClass("Whiz")
        .getProperty("account")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);

    final List<Account> result =
        database.command(new OSQLSynchQuery<Account>("select * from Account limit 1")).execute();

    final OIndex idx =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex((ODatabaseDocumentInternal) database.getUnderlying(), "Whiz.account");

    for (int i = 0; i < 5; i++) {
      final ODocument whiz = new ODocument("Whiz");

      whiz.field("id", i);
      whiz.field("text", "This is a test");
      whiz.field("account", result.get(0).getRid());

      whiz.save();
    }

    Assert.assertEquals(idx.getInternal().size(), 5);

    final List<ODocument> indexedResult =
        database
            .getUnderlying()
            .command(new OSQLSynchQuery<Profile>("select * from Whiz where account = ?"))
            .execute(result.get(0).getRid());

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
    try (ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL())) {
      db.open("admin", "admin");

      if (!db.getMetadata().getSchema().existsClass("TestClass")) {
        OClass testClass =
            db.getMetadata().getSchema().createClass("TestClass", 1, (OClass[]) null);
        OClass testLinkClass =
            db.getMetadata().getSchema().createClass("TestLinkClass", 1, (OClass[]) null);
        testClass
            .createProperty("testLink", OType.LINK, testLinkClass)
            .createIndex(INDEX_TYPE.NOTUNIQUE);
        testClass.createProperty("name", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
        testLinkClass.createProperty("testBoolean", OType.BOOLEAN);
        testLinkClass.createProperty("testString", OType.STRING);
      }
      ODocument testClassDocument = db.newInstance("TestClass");
      testClassDocument.field("name", "Test Class 1");
      ODocument testLinkClassDocument = new ODocument("TestLinkClass");
      testLinkClassDocument.field("testString", "Test Link Class 1");
      testLinkClassDocument.field("testBoolean", true);
      testClassDocument.field("testLink", testLinkClassDocument);
      testClassDocument.save();
      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.orient.core.id.ORecordId cannot be cast to
      // java.lang.Boolean
      List<ODocument> result =
          db.query(
              new OSQLSynchQuery<ODocument>(
                  "select from TestClass where testLink.testBoolean = true"));
      Assert.assertEquals(result.size(), 1);
      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.orient.core.id.ORecordId cannot be cast to
      // java.lang.String
      result =
          db.query(
              new OSQLSynchQuery<ODocument>(
                  "select from TestClass where testLink.testString = 'Test Link Class 1'"));
      Assert.assertEquals(result.size(), 1);
    }
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testLinkedIndexedPropertyInTx() {
    try (ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL())) {
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

      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.orient.core.id.ORecordId cannot be cast to
      // java.lang.Boolean
      List<ODocument> result =
          db.query(
              new OSQLSynchQuery<ODocument>(
                  "select from TestClass where testLink.testBoolean = true"));
      Assert.assertEquals(result.size(), 2);
      // THIS WILL THROW A java.lang.ClassCastException:
      // com.orientechnologies.orient.core.id.ORecordId cannot be cast to
      // java.lang.String
      result =
          db.query(
              new OSQLSynchQuery<ODocument>(
                  "select from TestClass where testLink.testString = 'Test Link Class 2'"));
      Assert.assertEquals(result.size(), 1);
    }
  }

  public void testDictionary() {
    try (ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL())) {
      db.open("admin", "admin");

      OClass pClass = db.getMetadata().getSchema().createClass("Person2", 1, (OClass[]) null);
      pClass.createProperty("firstName", OType.STRING);
      pClass.createProperty("lastName", OType.STRING);
      pClass.createProperty("age", OType.INTEGER);
      pClass.createIndex("testIdx", INDEX_TYPE.DICTIONARY, "firstName", "lastName");

      ODocument person = new ODocument("Person2");
      person.field("firstName", "foo").field("lastName", "bar").save();

      person = new ODocument("Person2");
      person.field("firstName", "foo").field("lastName", "bar").field("age", 32).save();
    }
  }

  public void testConcurrentRemoveDelete() {
    checkEmbeddedDB();

    try (ODatabaseDocumentInternal db = new ODatabaseDocumentTx(database.getURL())) {
      db.open("admin", "admin");

      if (!db.getMetadata().getSchema().existsClass("MyFruit")) {
        OClass fruitClass = db.getMetadata().getSchema().createClass("MyFruit", 1, (OClass[]) null);
        fruitClass.createProperty("name", OType.STRING);
        fruitClass.createProperty("color", OType.STRING);

        db.getMetadata()
            .getSchema()
            .getClass("MyFruit")
            .getProperty("name")
            .createIndex(INDEX_TYPE.UNIQUE);

        db.getMetadata()
            .getSchema()
            .getClass("MyFruit")
            .getProperty("color")
            .createIndex(INDEX_TYPE.NOTUNIQUE);
      }

      long expectedIndexSize = 0;

      final int passCount = 10;
      final int chunkSize = 10;

      for (int pass = 0; pass < passCount; pass++) {
        List<ODocument> recordsToDelete = new ArrayList<>();
        db.begin();
        for (int i = 0; i < chunkSize; i++) {
          ODocument d =
              new ODocument("MyFruit")
                  .field("name", "ABC" + pass + 'K' + i)
                  .field("color", "FOO" + pass);
          d.save();
          if (i < chunkSize / 2) {
            recordsToDelete.add(d);
          }
        }
        db.commit();

        expectedIndexSize += chunkSize;
        Assert.assertEquals(
            db.getMetadata()
                .getIndexManagerInternal()
                .getClassIndex(db, "MyFruit", "MyFruit.color")
                .getInternal()
                .size(),
            expectedIndexSize,
            "After add");

        // do delete
        db.begin();
        for (final ODocument recordToDelete : recordsToDelete) {
          Assert.assertNotNull(db.delete(recordToDelete));
        }
        db.commit();

        expectedIndexSize -= recordsToDelete.size();
        Assert.assertEquals(
            db.getMetadata()
                .getIndexManagerInternal()
                .getClassIndex(db, "MyFruit", "MyFruit.color")
                .getInternal()
                .size(),
            expectedIndexSize,
            "After delete");
      }
    }
  }

  public void testIndexParamsAutoConversion() {
    checkEmbeddedDB();

    final ODocument doc;
    final ORecordId result;
    try (ODatabaseDocumentInternal db = new ODatabaseDocumentTx(database.getURL())) {
      db.open("admin", "admin");

      if (!db.getMetadata().getSchema().existsClass("IndexTestTerm")) {
        final OClass termClass =
            db.getMetadata().getSchema().createClass("IndexTestTerm", 1, (OClass[]) null);
        termClass.createProperty("label", OType.STRING);
        termClass.createIndex(
            "idxTerm",
            INDEX_TYPE.UNIQUE.toString(),
            null,
            new ODocument().fields("ignoreNullValues", true),
            new String[] {"label"});
      }

      doc = new ODocument("IndexTestTerm");
      doc.field("label", "42");
      doc.save();

      try (Stream<ORID> stream =
          db.getMetadata()
              .getIndexManagerInternal()
              .getIndex(db, "idxTerm")
              .getInternal()
              .getRids("42")) {
        result = (ORecordId) stream.findAny().orElse(null);
      }
    }
    Assert.assertNotNull(result);
    Assert.assertEquals(result.getIdentity(), doc.getIdentity());
  }

  public void testTransactionUniqueIndexTestOne() {
    checkEmbeddedDB();

    ODatabaseDocumentInternal db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final OClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexTest", 1, (OClass[]) null);
      termClass.createProperty("label", OType.STRING);
      termClass.createIndex(
          "idxTransactionUniqueIndexTest",
          INDEX_TYPE.UNIQUE.toString(),
          null,
          new ODocument().fields("ignoreNullValues", true),
          new String[] {"label"});
    }

    ODocument docOne = new ODocument("TransactionUniqueIndexTest");
    docOne.field("label", "A");
    docOne.save();

    final OIndex index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "idxTransactionUniqueIndexTest");
    Assert.assertEquals(index.getInternal().size(), 1);

    db.begin();
    try {
      ODocument docTwo = new ODocument("TransactionUniqueIndexTest");
      docTwo.field("label", "A");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException ignored) {
    }

    Assert.assertEquals(index.getInternal().size(), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestOne")
  public void testTransactionUniqueIndexTestTwo() {
    checkEmbeddedDB();

    ODatabaseDocumentInternal db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexTest")) {
      final OClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexTest", 1, (OClass[]) null);
      termClass.createProperty("label", OType.STRING);
      termClass.createIndex(
          "idxTransactionUniqueIndexTest",
          INDEX_TYPE.UNIQUE.toString(),
          null,
          new ODocument().fields("ignoreNullValues", true),
          new String[] {"label"});
    }

    final OIndex index =
        ((ODatabaseDocumentInternal) db)
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "idxTransactionUniqueIndexTest");
    Assert.assertEquals(index.getInternal().size(), 1);

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
      db.rollback();
    }

    Assert.assertEquals(index.getInternal().size(), 1);
  }

  public void testTransactionUniqueIndexTestWithDotNameOne() {
    checkEmbeddedDB();

    ODatabaseDocumentInternal db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final OClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexWithDotTest", 1, (OClass[]) null);
      termClass.createProperty("label", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
    }

    ODocument docOne = new ODocument("TransactionUniqueIndexWithDotTest");
    docOne.field("label", "A");
    docOne.save();

    final OIndex index =
        ((ODatabaseDocumentInternal) db)
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "TransactionUniqueIndexWithDotTest.label");
    Assert.assertEquals(index.getInternal().size(), 1);

    long countClassBefore = db.countClass("TransactionUniqueIndexWithDotTest");
    db.begin();
    try {
      ODocument docTwo = new ODocument("TransactionUniqueIndexWithDotTest");
      docTwo.field("label", "A");
      docTwo.save();

      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException ignored) {
    }

    Assert.assertEquals(
        ((List<ODocument>)
                db.command(new OCommandSQL("select from TransactionUniqueIndexWithDotTest"))
                    .execute())
            .size(),
        countClassBefore);

    Assert.assertEquals(index.getInternal().size(), 1);
  }

  @Test(dependsOnMethods = "testTransactionUniqueIndexTestWithDotNameOne")
  public void testTransactionUniqueIndexTestWithDotNameTwo() {
    checkEmbeddedDB();

    ODatabaseDocumentInternal db = new ODatabaseDocumentTx(database.getURL());
    db.open("admin", "admin");

    if (!db.getMetadata().getSchema().existsClass("TransactionUniqueIndexWithDotTest")) {
      final OClass termClass =
          db.getMetadata()
              .getSchema()
              .createClass("TransactionUniqueIndexWithDotTest", 1, (OClass[]) null);
      termClass.createProperty("label", OType.STRING).createIndex(INDEX_TYPE.UNIQUE);
    }

    final OIndex index =
        ((ODatabaseDocumentInternal) db)
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "TransactionUniqueIndexWithDotTest.label");
    Assert.assertEquals(index.getInternal().size(), 1);

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
      db.rollback();
    }

    Assert.assertEquals(index.getInternal().size(), 1);
  }

  @Test(dependsOnMethods = "linkedIndexedProperty")
  public void testIndexRemoval() {
    checkEmbeddedDB();

    final OIndex index = getIndex("Profile.nick");

    Iterator<ORawPair<Object, ORID>> streamIterator;
    Object key;
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      streamIterator = stream.iterator();
      Assert.assertTrue(streamIterator.hasNext());

      ORawPair<Object, ORID> pair = streamIterator.next();
      key = pair.first;
      pair.second.getRecord().delete();
    }

    try (Stream<ORID> stream = index.getInternal().getRids(key)) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  public void createInheritanceIndex() {
    try (ODatabaseDocument db = new ODatabaseDocumentTx(database.getURL())) {
      db.open("admin", "admin");

      if (!db.getMetadata().getSchema().existsClass("BaseTestClass")) {
        OClass baseClass =
            db.getMetadata().getSchema().createClass("BaseTestClass", 1, (OClass[]) null);
        OClass childClass =
            db.getMetadata().getSchema().createClass("ChildTestClass", 1, (OClass[]) null);
        OClass anotherChildClass =
            db.getMetadata().getSchema().createClass("AnotherChildTestClass", 1, (OClass[]) null);

        if (!baseClass.isSuperClassOf(childClass)) childClass.setSuperClass(baseClass);
        if (!baseClass.isSuperClassOf(anotherChildClass))
          anotherChildClass.setSuperClass(baseClass);

        baseClass
            .createProperty("testParentProperty", OType.LONG)
            .createIndex(INDEX_TYPE.NOTUNIQUE);
      }

      ODocument childClassDocument = db.newInstance("ChildTestClass");
      childClassDocument.field("testParentProperty", 10L);
      childClassDocument.save();

      ODocument anotherChildClassDocument = db.newInstance("AnotherChildTestClass");
      anotherChildClassDocument.field("testParentProperty", 11L);
      anotherChildClassDocument.save();

      Assert.assertNotEquals(
          childClassDocument.getIdentity(), new ORecordId(-1, ORID.CLUSTER_POS_INVALID));
      Assert.assertNotEquals(
          anotherChildClassDocument.getIdentity(), new ORecordId(-1, ORID.CLUSTER_POS_INVALID));
    }
  }

  @Test(dependsOnMethods = "createInheritanceIndex")
  public void testIndexReturnOnlySpecifiedClass() {

    ODatabaseDocument db = database.getUnderlying();

    try (OResultSet result =
        db.command("select * from ChildTestClass where testParentProperty = 10")) {

      Assert.assertEquals(10L, result.next().<Object>getProperty("testParentProperty"));
      Assert.assertFalse(result.hasNext());
    }

    try (OResultSet result =
        db.command("select * from AnotherChildTestClass where testParentProperty = 11")) {
      Assert.assertEquals(11L, result.next().<Object>getProperty("testParentProperty"));
      Assert.assertFalse(result.hasNext());
    }
  }

  public void testNotUniqueIndexKeySize() {
    checkEmbeddedDB();

    final OSchema schema = database.getMetadata().getSchema();
    OClass cls = schema.createClass("IndexNotUniqueIndexKeySize");
    cls.createProperty("value", OType.INTEGER);
    cls.createIndex("IndexNotUniqueIndexKeySizeIndex", INDEX_TYPE.NOTUNIQUE, "value");

    OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();

    final OIndex idx =
        idxManager.getIndex(
            (ODatabaseDocumentInternal) database.getUnderlying(),
            "IndexNotUniqueIndexKeySizeIndex");

    final Set<Integer> keys = new HashSet<>();
    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      final ODocument doc = new ODocument("IndexNotUniqueIndexKeySize");
      doc.field("value", key);
      doc.save();
      keys.add(key);
    }

    try (Stream<ORawPair<Object, ORID>> stream = idx.getInternal().stream()) {
      Assert.assertEquals(stream.map((pair) -> pair.first).distinct().count(), keys.size());
    }
  }

  public void testNotUniqueIndexSize() {
    checkEmbeddedDB();

    final OSchema schema = database.getMetadata().getSchema();
    OClass cls = schema.createClass("IndexNotUniqueIndexSize");
    cls.createProperty("value", OType.INTEGER);
    cls.createIndex("IndexNotUniqueIndexSizeIndex", INDEX_TYPE.NOTUNIQUE, "value");

    OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
    final OIndex idx =
        idxManager.getIndex(
            (ODatabaseDocumentInternal) database.getUnderlying(), "IndexNotUniqueIndexSizeIndex");

    for (int i = 1; i < 100; i++) {
      final Integer key = (int) Math.log(i);

      final ODocument doc = new ODocument("IndexNotUniqueIndexSize");
      doc.field("value", key);
      doc.save();
    }

    Assert.assertEquals(idx.getInternal().size(), 99);
  }

  @Test
  public void testIndexRebuildDuringNonProxiedObjectDelete() {
    checkEmbeddedDB();

    Profile profile =
        new Profile(
            "NonProxiedObjectToDelete",
            "NonProxiedObjectToDelete",
            "NonProxiedObjectToDelete",
            null);
    profile = database.save(profile);

    OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
    OIndex nickIndex =
        idxManager.getIndex((ODatabaseDocumentInternal) database.getUnderlying(), "Profile.nick");

    try (Stream<ORID> stream = nickIndex.getInternal().getRids("NonProxiedObjectToDelete")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    final Profile loadedProfile = database.load(new ORecordId(profile.getId()));
    database.delete(database.<Object>detach(loadedProfile, true));

    try (Stream<ORID> stream = nickIndex.getInternal().getRids("NonProxiedObjectToDelete")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringNonProxiedObjectDelete")
  public void testIndexRebuildDuringDetachAllNonProxiedObjectDelete() {
    checkEmbeddedDB();

    Profile profile =
        new Profile(
            "NonProxiedObjectToDelete",
            "NonProxiedObjectToDelete",
            "NonProxiedObjectToDelete",
            null);
    profile = database.save(profile);

    OIndexManagerAbstract idxManager = database.getMetadata().getIndexManagerInternal();
    OIndex nickIndex =
        idxManager.getIndex((ODatabaseDocumentInternal) database.getUnderlying(), "Profile.nick");

    try (Stream<ORID> stream = nickIndex.getInternal().getRids("NonProxiedObjectToDelete")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    final Profile loadedProfile = database.load(new ORecordId(profile.getId()));
    database.delete(database.<Object>detachAll(loadedProfile, true));

    try (Stream<ORID> stream = nickIndex.getInternal().getRids("NonProxiedObjectToDelete")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
  }

  @Test(dependsOnMethods = "testIndexRebuildDuringDetachAllNonProxiedObjectDelete")
  public void testRestoreUniqueIndex() {
    database.getMetadata().getSchema().getClass("Profile").getProperty("nick").dropIndexes();
    database
        .command(
            "CREATE INDEX Profile.nick on Profile (nick) UNIQUE METADATA {ignoreNullValues: true}")
        .close();
    database.getMetadata().reload();
  }

  @Test
  public void testIndexInCompositeQuery() {
    OClass classOne =
        database.getMetadata().getSchema().createClass("CompoundSQLIndexTest1", 1, (OClass[]) null);
    OClass classTwo =
        database.getMetadata().getSchema().createClass("CompoundSQLIndexTest2", 1, (OClass[]) null);

    classTwo.createProperty("address", OType.LINK, classOne);

    classTwo.createIndex("CompoundSQLIndexTestIndex", INDEX_TYPE.UNIQUE, "address");

    ODocument docOne = new ODocument("CompoundSQLIndexTest1");
    docOne.field("city", "Montreal");

    docOne.save();

    ODocument docTwo = new ODocument("CompoundSQLIndexTest2");
    docTwo.field("address", docOne);
    docTwo.save();

    List<ODocument> result =
        database
            .getUnderlying()
            .query(
                new OSQLSynchQuery<ODocument>(
                    "select from CompoundSQLIndexTest2 where address in (select from CompoundSQLIndexTest1 where city='Montreal')"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).getIdentity(), docTwo.getIdentity());
  }

  public void testIndexWithLimitAndOffset() {
    ODatabaseDocument databaseDocumentTx = database.getUnderlying();

    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    final OClass indexWithLimitAndOffset =
        schema.createClass("IndexWithLimitAndOffsetClass", 1, (OClass[]) null);
    indexWithLimitAndOffset.createProperty("val", OType.INTEGER);
    indexWithLimitAndOffset.createProperty("index", OType.INTEGER);

    databaseDocumentTx
        .command(
            "create index IndexWithLimitAndOffset on IndexWithLimitAndOffsetClass (val) notunique")
        .close();

    for (int i = 0; i < 30; i++) {
      final ODocument document = new ODocument("IndexWithLimitAndOffsetClass");
      document.field("val", i / 10);
      document.field("index", i);
      document.save();
    }

    final List<ODocument> result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from IndexWithLimitAndOffsetClass where val = 1 offset 5 limit 2"));
    Assert.assertEquals(result.size(), 2);

    for (int i = 0; i < 2; i++) {
      final ODocument document = result.get(i);
      Assert.assertEquals(document.<Object>field("val"), 1);
      Assert.assertEquals(document.<Object>field("index"), 15 + i);
    }
  }

  public void testNullIndexKeysSupport() {
    final ODatabaseDocument databaseDocumentTx = database.getUnderlying();

    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    final OClass clazz = schema.createClass("NullIndexKeysSupport", 1, (OClass[]) null);
    clazz.createProperty("nullField", OType.STRING);

    ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "NullIndexKeysSupportIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"nullField"});
    for (int i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        ODocument document = new ODocument("NullIndexKeysSupport");
        document.field("nullField", (Object) null);
        document.save();
      } else {
        ODocument document = new ODocument("NullIndexKeysSupport");
        document.field("nullField", "val" + i);
        document.save();
      }
    }

    List<ODocument> result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullIndexKeysSupport where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullIndexKeysSupport where nullField is null";
    result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullIndexKeysSupport where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (ODocument document : result) Assert.assertNull(document.field("nullField"));

    final ODocument explain =
        databaseDocumentTx.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullIndexKeysSupportIndex"));
  }

  public void testNullHashIndexKeysSupport() {
    final ODatabaseDocument databaseDocumentTx = database.getUnderlying();

    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    final OClass clazz = schema.createClass("NullHashIndexKeysSupport", 1, (OClass[]) null);
    clazz.createProperty("nullField", OType.STRING);

    ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "NullHashIndexKeysSupportIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"nullField"});
    for (int i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        ODocument document = new ODocument("NullHashIndexKeysSupport");
        document.field("nullField", (Object) null);
        document.save();
      } else {
        ODocument document = new ODocument("NullHashIndexKeysSupport");
        document.field("nullField", "val" + i);
        document.save();
      }
    }

    List<ODocument> result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullHashIndexKeysSupport where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullHashIndexKeysSupport where nullField is null";
    result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullHashIndexKeysSupport where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (ODocument document : result) Assert.assertNull(document.field("nullField"));

    final ODocument explain =
        databaseDocumentTx.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullHashIndexKeysSupportIndex"));
  }

  public void testNullIndexKeysSupportInTx() {
    final ODatabaseDocument databaseDocumentTx = database.getUnderlying();

    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    final OClass clazz = schema.createClass("NullIndexKeysSupportInTx", 1, (OClass[]) null);
    clazz.createProperty("nullField", OType.STRING);

    ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "NullIndexKeysSupportInTxIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"nullField"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        ODocument document = new ODocument("NullIndexKeysSupportInTx");
        document.field("nullField", (Object) null);
        document.save();
      } else {
        ODocument document = new ODocument("NullIndexKeysSupportInTx");
        document.field("nullField", "val" + i);
        document.save();
      }
    }

    database.commit();

    List<ODocument> result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullIndexKeysSupportInTx where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullIndexKeysSupportInTx where nullField is null";
    result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullIndexKeysSupportInTx where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (ODocument document : result) Assert.assertNull(document.field("nullField"));

    final ODocument explain =
        databaseDocumentTx.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(
        explain.<Set<String>>field("involvedIndexes").contains("NullIndexKeysSupportInTxIndex"));
  }

  public void testNullIndexKeysSupportInMiddleTx() {
    if (database.getURL().startsWith("remote:")) return;

    final ODatabaseDocument databaseDocumentTx = database.getUnderlying();

    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    final OClass clazz = schema.createClass("NullIndexKeysSupportInMiddleTx", 1, (OClass[]) null);
    clazz.createProperty("nullField", OType.STRING);

    ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    clazz.createIndex(
        "NullIndexKeysSupportInMiddleTxIndex",
        INDEX_TYPE.NOTUNIQUE.toString(),
        null,
        metadata,
        new String[] {"nullField"});

    database.begin();

    for (int i = 0; i < 20; i++) {
      if (i % 5 == 0) {
        ODocument document = new ODocument("NullIndexKeysSupportInMiddleTx");
        document.field("nullField", (Object) null);
        document.save();
      } else {
        ODocument document = new ODocument("NullIndexKeysSupportInMiddleTx");
        document.field("nullField", "val" + i);
        document.save();
      }
    }

    List<ODocument> result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullIndexKeysSupportInMiddleTx where nullField = 'val3'"));
    Assert.assertEquals(result.size(), 1);

    Assert.assertEquals(result.get(0).field("nullField"), "val3");

    final String query = "select from NullIndexKeysSupportInMiddleTx where nullField is null";
    result =
        databaseDocumentTx.query(
            new OSQLSynchQuery<ODocument>(
                "select from NullIndexKeysSupportInMiddleTx where nullField is null"));

    Assert.assertEquals(result.size(), 4);
    for (ODocument document : result) Assert.assertNull(document.field("nullField"));

    final ODocument explain =
        databaseDocumentTx.command(new OCommandSQL("explain " + query)).execute();
    Assert.assertTrue(
        explain
            .<Set<String>>field("involvedIndexes")
            .contains("NullIndexKeysSupportInMiddleTxIndex"));

    database.commit();
  }

  public void testCreateIndexAbstractClass() {
    final ODatabaseDocument databaseDocumentTx = database.getUnderlying();
    final OSchema schema = databaseDocumentTx.getMetadata().getSchema();

    OClass abstractClass = schema.createAbstractClass("TestCreateIndexAbstractClass");
    abstractClass
        .createProperty("value", OType.STRING)
        .setMandatory(true)
        .createIndex(INDEX_TYPE.UNIQUE);

    schema.createClass("TestCreateIndexAbstractClassChildOne", abstractClass);
    schema.createClass("TestCreateIndexAbstractClassChildTwo", abstractClass);

    ODocument docOne = new ODocument("TestCreateIndexAbstractClassChildOne");
    docOne.field("value", "val1");
    docOne.save();

    ODocument docTwo = new ODocument("TestCreateIndexAbstractClassChildTwo");
    docTwo.field("value", "val2");
    docTwo.save();

    final String queryOne = "select from TestCreateIndexAbstractClass where value = 'val1'";

    List<ODocument> resultOne = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(queryOne));
    Assert.assertEquals(resultOne.size(), 1);
    Assert.assertEquals(resultOne.get(0).getIdentity(), docOne.getIdentity());

    ODocument explain =
        databaseDocumentTx.command(new OCommandSQL("explain " + queryOne)).execute();
    Assert.assertTrue(
        explain
            .<Collection<String>>field("involvedIndexes")
            .contains("TestCreateIndexAbstractClass.value"));

    final String queryTwo = "select from TestCreateIndexAbstractClass where value = 'val2'";

    List<ODocument> resultTwo = databaseDocumentTx.query(new OSQLSynchQuery<ODocument>(queryTwo));
    Assert.assertEquals(resultTwo.size(), 1);
    Assert.assertEquals(resultTwo.get(0).getIdentity(), docTwo.getIdentity());

    explain = databaseDocumentTx.command(new OCommandSQL("explain " + queryTwo)).execute();
    Assert.assertTrue(
        explain
            .<Collection<String>>field("involvedIndexes")
            .contains("TestCreateIndexAbstractClass.value"));
  }

  @Test(enabled = false)
  public void testValuesContainerIsRemovedIfIndexIsRemoved() {
    if (database.getURL().startsWith("remote:")) return;

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz =
        schema.createClass("ValuesContainerIsRemovedIfIndexIsRemovedClass", 1, (OClass[]) null);
    clazz.createProperty("val", OType.STRING);

    database
        .command(
            "create index ValuesContainerIsRemovedIfIndexIsRemovedIndex on ValuesContainerIsRemovedIfIndexIsRemovedClass (val) notunique")
        .close();

    for (int i = 0; i < 10; i++) {
      for (int j = 0; j < 100; j++) {
        ODocument document = new ODocument("ValuesContainerIsRemovedIfIndexIsRemovedClass");
        document.field("val", "value" + i);
        document.save();
      }
    }

    final OAbstractPaginatedStorage storageLocalAbstract =
        (OAbstractPaginatedStorage)
            ((ODatabaseDocumentInternal) database.getUnderlying()).getStorage();

    final OWriteCache writeCache = storageLocalAbstract.getWriteCache();
    Assert.assertTrue(writeCache.exists("ValuesContainerIsRemovedIfIndexIsRemovedIndex.irs"));
    database.command("drop index ValuesContainerIsRemovedIfIndexIsRemovedIndex").close();
    Assert.assertFalse(writeCache.exists("ValuesContainerIsRemovedIfIndexIsRemovedIndex.irs"));
  }

  public void testPreservingIdentityInIndexTx() {
    checkEmbeddedDB();

    OrientGraph graph = new OrientGraph((ODatabaseDocumentInternal) database.getUnderlying(), true);
    graph.setAutoScaleEdgeType(true);

    OrientVertexType fieldClass = graph.getVertexType("PreservingIdentityInIndexTxChild");
    if (fieldClass == null) {
      fieldClass = graph.createVertexType("PreservingIdentityInIndexTxChild");
      fieldClass.createProperty("name", OType.STRING);
      fieldClass.createProperty("in_field", OType.LINK);
      fieldClass.createIndex("nameParentIndex", OClass.INDEX_TYPE.NOTUNIQUE, "in_field", "name");
    }

    Vertex parent = graph.addVertex("class:PreservingIdentityInIndexTxParent");
    Vertex child = graph.addVertex("class:PreservingIdentityInIndexTxChild");
    parent.addEdge("preservingIdentityInIndexTxEdge", child);
    child.setProperty("name", "pokus");

    Vertex parent2 = graph.addVertex("class:PreservingIdentityInIndexTxParent");
    Vertex child2 = graph.addVertex("class:PreservingIdentityInIndexTxChild");
    parent2.addEdge("preservingIdentityInIndexTxEdge", child2);
    child2.setProperty("name", "pokus2");
    graph.commit();

    {
      fieldClass = graph.getVertexType("PreservingIdentityInIndexTxChild");
      OIndex index = fieldClass.getClassIndex("nameParentIndex");
      OCompositeKey key = new OCompositeKey(parent.getId(), "pokus");

      Collection<ORID> h;
      try (Stream<ORID> stream = index.getInternal().getRids(key)) {
        h = stream.collect(Collectors.toList());
      }
      for (ORID o : h) {
        Assert.assertNotNull(graph.getVertex(o));
      }
    }

    {
      fieldClass = graph.getVertexType("PreservingIdentityInIndexTxChild");
      OIndex index = fieldClass.getClassIndex("nameParentIndex");
      OCompositeKey key = new OCompositeKey(parent2.getId(), "pokus2");

      Collection<ORID> h;
      try (Stream<ORID> stream = index.getInternal().getRids(key)) {
        h = stream.collect(Collectors.toList());
      }
      for (ORID o : h) {
        Assert.assertNotNull(graph.getVertex(o));
      }
    }

    parent.remove();
    child.remove();

    parent2.remove();
    child2.remove();

    graph.shutdown();
  }

  public void testEmptyNotUniqueIndex() {
    checkEmbeddedDB();

    OClass emptyNotUniqueIndexClazz =
        database
            .getMetadata()
            .getSchema()
            .createClass("EmptyNotUniqueIndexTest", 1, (OClass[]) null);
    emptyNotUniqueIndexClazz.createProperty("prop", OType.STRING);

    final OIndex notUniqueIndex =
        emptyNotUniqueIndexClazz.createIndex(
            "EmptyNotUniqueIndexTestIndex", INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "prop");
    ODocument document = new ODocument("EmptyNotUniqueIndexTest");
    document.field("prop", "keyOne");
    document.save();

    document = new ODocument("EmptyNotUniqueIndexTest");
    document.field("prop", "keyTwo");
    document.save();

    try (Stream<ORID> stream = notUniqueIndex.getInternal().getRids("RandomKeyOne")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = notUniqueIndex.getInternal().getRids("keyOne")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }

    try (Stream<ORID> stream = notUniqueIndex.getInternal().getRids("RandomKeyTwo")) {
      Assert.assertFalse(stream.findAny().isPresent());
    }
    try (Stream<ORID> stream = notUniqueIndex.getInternal().getRids("keyTwo")) {
      Assert.assertTrue(stream.findAny().isPresent());
    }
  }

  public void testNullIteration() {
    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) this.database.getUnderlying();

    OClass v = database.getMetadata().getSchema().getClass("V");
    OClass testNullIteration =
        database.getMetadata().getSchema().createClass("NullIterationTest", v);
    testNullIteration.createProperty("name", OType.STRING);
    testNullIteration.createProperty("birth", OType.DATETIME);

    database
        .command("CREATE VERTEX NullIterationTest SET name = 'Andrew', birth = sysdate()")
        .close();
    database
        .command("CREATE VERTEX NullIterationTest SET name = 'Marcel', birth = sysdate()")
        .close();
    database.command("CREATE VERTEX NullIterationTest SET name = 'Olivier'").close();

    ODocument metadata = new ODocument();
    metadata.field("ignoreNullValues", false);

    testNullIteration.createIndex(
        "NullIterationTestIndex",
        INDEX_TYPE.NOTUNIQUE.name(),
        null,
        metadata,
        new String[] {"birth"});

    OResultSet result = database.query("SELECT FROM NullIterationTest ORDER BY birth ASC");
    Assert.assertEquals(result.stream().count(), 3);

    result = database.query("SELECT FROM NullIterationTest ORDER BY birth DESC");
    Assert.assertEquals(result.stream().count(), 3);

    result = database.query("SELECT FROM NullIterationTest");
    Assert.assertEquals(result.stream().count(), 3);
  }

  public void testMultikeyWithoutFieldAndNullSupport() {
    checkEmbeddedDB();

    // generates stubs for index
    ODocument doc1 = new ODocument();
    doc1.save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument doc2 = new ODocument();
    doc2.save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument doc3 = new ODocument();
    doc3.save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument doc4 = new ODocument();
    doc4.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid1 = doc1.getIdentity();
    final ORID rid2 = doc2.getIdentity();
    final ORID rid3 = doc3.getIdentity();
    final ORID rid4 = doc4.getIdentity();

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) this.database.getUnderlying();

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("TestMultikeyWithoutField");

    clazz.createProperty("state", OType.BYTE);
    clazz.createProperty("users", OType.LINKSET);
    clazz.createProperty("time", OType.LONG);
    clazz.createProperty("reg", OType.LONG);
    clazz.createProperty("no", OType.INTEGER);

    final ODocument mt = new ODocument().field("ignoreNullValues", false);
    clazz.createIndex(
        "MultikeyWithoutFieldIndex",
        INDEX_TYPE.UNIQUE.toString(),
        null,
        mt,
        new String[] {"state", "users", "time", "reg", "no"});

    ODocument document = new ODocument("TestMultikeyWithoutField");
    document.field("state", (byte) 1);

    Set<ORID> users = new HashSet<>();
    users.add(rid1);
    users.add(rid2);

    document.field("users", users);
    document.field("time", 12L);
    document.field("reg", 14L);
    document.field("no", 12);

    document.save();

    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    // we support first and last keys check only for embedded storage
    if (!(database.isRemote())) {
      try (Stream<Object> keyStreamAsc = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStreamAsc.iterator().next(), new OCompositeKey((byte) 1, rid1, 12L, 14L, 12));
      }

      try (Stream<ORawPair<Object, ORID>> descStream = index.getInternal().descStream()) {
        Assert.assertEquals(
            descStream.iterator().next().first, new OCompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    final ORID rid = document.getIdentity();

    database.close();
    database.open("admin", "admin");

    document = database.load(rid);

    users = document.field("users");
    users.remove(rid1);
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(), 1);
    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    document = database.load(rid);

    users = document.field("users");
    users.remove(rid2);
    Assert.assertTrue(users.isEmpty());
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndex");

    Assert.assertEquals(index.getInternal().size(), 1);
    if (!(database.isRemote())) {
      try (Stream<Object> keyStreamAsc = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStreamAsc.iterator().next(), new OCompositeKey((byte) 1, null, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    document = database.load(rid);
    users = document.field("users");
    users.add(rid3);
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndex");

    Assert.assertEquals(index.getInternal().size(), 1);
    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    users = document.field("users");
    users.add(rid4);
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(), 2);

    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
      try (Stream<ORawPair<Object, ORID>> descStream = index.getInternal().descStream()) {
        Assert.assertEquals(
            descStream.iterator().next().first, new OCompositeKey((byte) 1, rid4, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    document.removeField("users");
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndex");
    Assert.assertEquals(index.getInternal().size(), 1);

    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, null, 12L, 14L, 12));
      }
    }
  }

  public void testMultikeyWithoutFieldAndNoNullSupport() {
    checkEmbeddedDB();

    // generates stubs for index
    ODocument doc1 = new ODocument();
    doc1.save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument doc2 = new ODocument();
    doc2.save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument doc3 = new ODocument();
    doc3.save(database.getClusterNameById(database.getDefaultClusterId()));
    ODocument doc4 = new ODocument();
    doc4.save(database.getClusterNameById(database.getDefaultClusterId()));

    final ORID rid1 = doc1.getIdentity();
    final ORID rid2 = doc2.getIdentity();
    final ORID rid3 = doc3.getIdentity();
    final ORID rid4 = doc4.getIdentity();

    ODatabaseDocumentInternal database = (ODatabaseDocumentInternal) this.database.getUnderlying();

    final OSchema schema = database.getMetadata().getSchema();
    OClass clazz = schema.createClass("TestMultikeyWithoutFieldNoNullSupport");

    clazz.createProperty("state", OType.BYTE);
    clazz.createProperty("users", OType.LINKSET);
    clazz.createProperty("time", OType.LONG);
    clazz.createProperty("reg", OType.LONG);
    clazz.createProperty("no", OType.INTEGER);

    clazz.createIndex(
        "MultikeyWithoutFieldIndexNoNullSupport",
        INDEX_TYPE.UNIQUE.toString(),
        null,
        new ODocument().fields("ignoreNullValues", true),
        new String[] {"state", "users", "time", "reg", "no"});

    ODocument document = new ODocument("TestMultikeyWithoutFieldNoNullSupport");
    document.field("state", (byte) 1);

    Set<ORID> users = new HashSet<>();
    users.add(rid1);
    users.add(rid2);

    document.field("users", users);
    document.field("time", 12L);
    document.field("reg", 14L);
    document.field("no", 12);

    document.save();

    OIndex index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(), 2);

    // we support first and last keys check only for embedded storage
    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid1, 12L, 14L, 12));
      }
      try (Stream<ORawPair<Object, ORID>> descStream = index.getInternal().descStream()) {
        Assert.assertEquals(
            descStream.iterator().next().first, new OCompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    final ORID rid = document.getIdentity();

    database.close();
    database.open("admin", "admin");

    document = database.load(rid);

    users = document.field("users");
    users.remove(rid1);
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(), 1);
    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid2, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    document = database.load(rid);

    users = document.field("users");
    users.remove(rid2);
    Assert.assertTrue(users.isEmpty());

    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(), 0);

    database.close();
    database.open("admin", "admin");

    document = database.load(rid);
    users = document.field("users");
    users.add(rid3);
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(), 1);

    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    users = document.field("users");
    users.add(rid4);
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(), 2);

    if (!(database.isRemote())) {
      try (Stream<Object> keyStream = index.getInternal().keyStream()) {
        Assert.assertEquals(
            keyStream.iterator().next(), new OCompositeKey((byte) 1, rid3, 12L, 14L, 12));
      }
      try (Stream<ORawPair<Object, ORID>> descStream = index.getInternal().descStream()) {
        Assert.assertEquals(
            descStream.iterator().next().first, new OCompositeKey((byte) 1, rid4, 12L, 14L, 12));
      }
    }

    database.close();
    database.open("admin", "admin");

    document.removeField("users");
    document.save();

    index =
        database
            .getMetadata()
            .getIndexManagerInternal()
            .getIndex(database, "MultikeyWithoutFieldIndexNoNullSupport");
    Assert.assertEquals(index.getInternal().size(), 0);
  }

  public void testNullValuesCountSBTreeUnique() {
    checkEmbeddedDB();

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.getUnderlying();

    OClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountSBTreeUnique");
    nullSBTreeClass.createProperty("field", OType.INTEGER);
    nullSBTreeClass.createIndex("NullValuesCountSBTreeUniqueIndex", INDEX_TYPE.UNIQUE, "field");

    ODocument docOne = new ODocument("NullValuesCountSBTreeUnique");
    docOne.field("field", 1);
    docOne.save();

    ODocument docTwo = new ODocument("NullValuesCountSBTreeUnique");
    docTwo.field("field", (Integer) null);
    docTwo.save();

    OIndex index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "NullValuesCountSBTreeUniqueIndex");
    Assert.assertEquals(index.getInternal().size(), 2);
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      try (Stream<ORID> nullStream = index.getInternal().getRids(null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountSBTreeNotUniqueOne() {
    checkEmbeddedDB();

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.getUnderlying();

    OClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountSBTreeNotUniqueOne");
    nullSBTreeClass.createProperty("field", OType.INTEGER);
    nullSBTreeClass.createIndex(
        "NullValuesCountSBTreeNotUniqueOneIndex", INDEX_TYPE.NOTUNIQUE, "field");

    ODocument docOne = new ODocument("NullValuesCountSBTreeNotUniqueOne");
    docOne.field("field", 1);
    docOne.save();

    ODocument docTwo = new ODocument("NullValuesCountSBTreeNotUniqueOne");
    docTwo.field("field", (Integer) null);
    docTwo.save();

    OIndex index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountSBTreeNotUniqueOneIndex");
    Assert.assertEquals(index.getInternal().size(), 2);
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      try (Stream<ORID> nullStream = index.getInternal().getRids(null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountSBTreeNotUniqueTwo() {
    checkEmbeddedDB();

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.getUnderlying();

    OClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountSBTreeNotUniqueTwo");
    nullSBTreeClass.createProperty("field", OType.INTEGER);
    nullSBTreeClass.createIndex(
        "NullValuesCountSBTreeNotUniqueTwoIndex", INDEX_TYPE.NOTUNIQUE, "field");

    ODocument docOne = new ODocument("NullValuesCountSBTreeNotUniqueTwo");
    docOne.field("field", (Integer) null);
    docOne.save();

    ODocument docTwo = new ODocument("NullValuesCountSBTreeNotUniqueTwo");
    docTwo.field("field", (Integer) null);
    docTwo.save();

    OIndex index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountSBTreeNotUniqueTwoIndex");
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      try (Stream<ORID> nullStream = index.getInternal().getRids(null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count()
                + nullStream.findAny().map(v -> 1).orElse(0),
            1);
      }
    }
    Assert.assertEquals(index.getInternal().size(), 2);
  }

  public void testNullValuesCountHashUnique() {
    checkEmbeddedDB();

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.getUnderlying();

    OClass nullSBTreeClass = db.getMetadata().getSchema().createClass("NullValuesCountHashUnique");
    nullSBTreeClass.createProperty("field", OType.INTEGER);
    nullSBTreeClass.createIndex(
        "NullValuesCountHashUniqueIndex", INDEX_TYPE.UNIQUE_HASH_INDEX, "field");

    ODocument docOne = new ODocument("NullValuesCountHashUnique");
    docOne.field("field", 1);
    docOne.save();

    ODocument docTwo = new ODocument("NullValuesCountHashUnique");
    docTwo.field("field", (Integer) null);
    docTwo.save();

    OIndex index =
        db.getMetadata().getIndexManagerInternal().getIndex(db, "NullValuesCountHashUniqueIndex");
    Assert.assertEquals(index.getInternal().size(), 2);
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      try (Stream<ORID> nullStream = index.getInternal().getRids(null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountHashNotUniqueOne() {
    checkEmbeddedDB();

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.getUnderlying();

    OClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountHashNotUniqueOne");
    nullSBTreeClass.createProperty("field", OType.INTEGER);
    nullSBTreeClass.createIndex(
        "NullValuesCountHashNotUniqueOneIndex", INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "field");

    ODocument docOne = new ODocument("NullValuesCountHashNotUniqueOne");
    docOne.field("field", 1);
    docOne.save();

    ODocument docTwo = new ODocument("NullValuesCountHashNotUniqueOne");
    docTwo.field("field", (Integer) null);
    docTwo.save();

    OIndex index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountHashNotUniqueOneIndex");
    Assert.assertEquals(index.getInternal().size(), 2);
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      try (Stream<ORID> nullStream = index.getInternal().getRids(null)) {
        Assert.assertEquals(
            stream.map((pair) -> pair.first).distinct().count() + nullStream.count(), 2);
      }
    }
  }

  public void testNullValuesCountHashNotUniqueTwo() {
    checkEmbeddedDB();

    final ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) database.getUnderlying();

    OClass nullSBTreeClass =
        db.getMetadata().getSchema().createClass("NullValuesCountHashNotUniqueTwo");
    nullSBTreeClass.createProperty("field", OType.INTEGER);
    nullSBTreeClass.createIndex(
        "NullValuesCountHashNotUniqueTwoIndex", INDEX_TYPE.NOTUNIQUE_HASH_INDEX, "field");

    ODocument docOne = new ODocument("NullValuesCountHashNotUniqueTwo");
    docOne.field("field", (Integer) null);
    docOne.save();

    ODocument docTwo = new ODocument("NullValuesCountHashNotUniqueTwo");
    docTwo.field("field", (Integer) null);
    docTwo.save();

    OIndex index =
        db.getMetadata()
            .getIndexManagerInternal()
            .getIndex(db, "NullValuesCountHashNotUniqueTwoIndex");
    try (Stream<ORawPair<Object, ORID>> stream = index.getInternal().stream()) {
      try (Stream<ORID> nullStream = index.getInternal().getRids(null)) {
        Assert.assertEquals(
            stream.map(pair -> pair.first).distinct().count()
                + nullStream.findAny().map(v -> 1).orElse(0),
            1);
      }
    }
    Assert.assertEquals(index.getInternal().size(), 2);
  }

  @Test
  public void testParamsOrder() {

    OrientBaseGraph graph =
        new OrientGraphNoTx("memory:IndexTest_testParamsOrder", "admin", "admin");
    ODatabaseDocument database = graph.getRawGraph();

    database.command("CREATE CLASS Task extends V").close();
    database
        .command("CREATE PROPERTY Task.projectId STRING (MANDATORY TRUE, NOTNULL, MAX 20)")
        .close();
    database.command("CREATE PROPERTY Task.seq SHORT ( MANDATORY TRUE, NOTNULL, MIN 0)").close();
    database.command("CREATE INDEX TaskPK ON Task (projectId, seq) UNIQUE").close();

    database.command("INSERT INTO Task (projectId, seq) values ( 'foo', 2)").close();
    database.command("INSERT INTO Task (projectId, seq) values ( 'bar', 3)").close();
    Iterable<Vertex> x =
        graph.getVertices(
            "Task", new String[] {"seq", "projectId"}, new Object[] {(short) 2, "foo"});
    Iterator<Vertex> iter = x.iterator();
    Assert.assertTrue(iter.hasNext());
    iter.next();
    Assert.assertFalse(iter.hasNext());
    graph.drop();
  }
}
