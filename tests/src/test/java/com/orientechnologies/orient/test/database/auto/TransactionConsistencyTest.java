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

import static com.orientechnologies.DatabaseAbstractTest.getEnvironment;

import com.orientechnologies.DatabaseAbstractTest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;
import com.orientechnologies.orient.test.domain.business.Account;
import com.orientechnologies.orient.test.domain.business.Address;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class TransactionConsistencyTest extends DocumentDBBaseTest {
  protected ODatabaseDocumentTx database1;
  protected ODatabaseDocumentTx database2;

  public static final String NAME = "name";

  @Parameters(value = "url")
  public TransactionConsistencyTest(@Optional String url) {
    super(url);
    setAutoManageDatabase(false);
  }

  @Test
  public void test1RollbackOnConcurrentException() throws IOException {
    database1 = new ODatabaseDocumentTx(url).open("admin", "admin");

    database1.begin(TXTYPE.OPTIMISTIC);

    // Create docA.
    ODocument vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.save(vDocA_db1, database1.getClusterNameById(database1.getDefaultClusterId()));

    // Create docB.
    ODocument vDocB_db1 = database1.newInstance();
    vDocB_db1.field(NAME, "docB");
    database1.save(vDocB_db1, database1.getClusterNameById(database1.getDefaultClusterId()));

    database1.commit();

    // Keep the IDs.
    ORID vDocA_Rid = vDocA_db1.getIdentity().copy();
    ORID vDocB_Rid = vDocB_db1.getIdentity().copy();

    int vDocA_version = -1;
    int vDocB_version = -1;

    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
    database2.begin(TXTYPE.OPTIMISTIC);
    try {
      // Get docA and update in db2 transaction context
      ODocument vDocA_db2 = database2.load(vDocA_Rid);
      vDocA_db2.field(NAME, "docA_v2");
      database2.save(vDocA_db2);

      // Concurrent update docA via database1 -> will throw OConcurrentModificationException at
      // database2.commit().
      database1.activateOnCurrentThread();
      database1.begin(TXTYPE.OPTIMISTIC);
      try {
        vDocA_db1.field(NAME, "docA_v3");
        database1.save(vDocA_db1);
        database1.commit();
      } catch (OConcurrentModificationException e) {
        Assert.fail("Should not failed here...");
      }
      Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");
      // Keep the last versions.
      // Following updates should failed and reverted.
      vDocA_version = vDocA_db1.getVersion();
      vDocB_version = vDocB_db1.getVersion();

      // Update docB in db2 transaction context -> should be rollbacked.
      database2.activateOnCurrentThread();
      ODocument vDocB_db2 = database2.load(vDocB_Rid);
      vDocB_db2.field(NAME, "docB_UpdatedInTranscationThatWillBeRollbacked");
      database2.save(vDocB_db2);

      // Will throw OConcurrentModificationException
      database2.commit();
      Assert.fail("Should throw OConcurrentModificationException");
    } catch (OConcurrentModificationException e) {
      database2.rollback();
    }

    // Force reload all (to be sure it is not a cache problem)
    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.getStorage().close();
    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument vDocA_db2 = database2.load(vDocA_Rid);
    Assert.assertEquals(vDocA_db2.field(NAME), "docA_v3");
    Assert.assertEquals(vDocA_db2.getVersion(), vDocA_version);

    // docB should be in the first state : "docB"
    ODocument vDocB_db2 = database2.load(vDocB_Rid);
    Assert.assertEquals(vDocB_db2.field(NAME), "docB");
    Assert.assertEquals(vDocB_db2.getVersion(), vDocB_version);

    database2.close();
  }

  @Test
  public void test4RollbackWithPin() throws IOException {
    database1 = new ODatabaseDocumentTx(url).open("admin", "admin");

    // Create docA.
    ODocument vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.save(vDocA_db1, database1.getClusterNameById(database1.getDefaultClusterId()));

    // Keep the IDs.
    ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
    database2.begin(TXTYPE.OPTIMISTIC);
    try {
      // Get docA and update in db2 transaction context
      ODocument vDocA_db2 = database2.load(vDocA_Rid);
      vDocA_db2.field(NAME, "docA_v2");
      database2.save(vDocA_db2);

      database1.activateOnCurrentThread();
      database1.begin(TXTYPE.OPTIMISTIC);
      try {
        vDocA_db1.field(NAME, "docA_v3");
        database1.save(vDocA_db1);
        database1.commit();
      } catch (OConcurrentModificationException e) {
        Assert.fail("Should not failed here...");
      }
      Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

      // Will throw OConcurrentModificationException
      database2.activateOnCurrentThread();
      database2.commit();
      Assert.fail("Should throw OConcurrentModificationException");
    } catch (OConcurrentModificationException e) {
      database2.rollback();
    }

    // Force reload all (to be sure it is not a cache problem)
    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

    // docB should be in the last state : "docA_v3"
    ODocument vDocB_db2 = database2.load(vDocA_Rid);
    Assert.assertEquals(vDocB_db2.field(NAME), "docA_v3");

    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
  }

  @Test
  public void test3RollbackWithCopyCacheStrategy() throws IOException {
    database1 = new ODatabaseDocumentTx(url).open("admin", "admin");

    // Create docA.
    ODocument vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.save(vDocA_db1, database1.getClusterNameById(database1.getDefaultClusterId()));

    // Keep the IDs.
    ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
    database2.begin(TXTYPE.OPTIMISTIC);
    try {
      // Get docA and update in db2 transaction context
      ODocument vDocA_db2 = database2.load(vDocA_Rid);
      vDocA_db2.field(NAME, "docA_v2");
      database2.save(vDocA_db2);

      database1.activateOnCurrentThread();
      database1.begin(TXTYPE.OPTIMISTIC);
      try {
        vDocA_db1.field(NAME, "docA_v3");
        database1.save(vDocA_db1);
        database1.commit();
      } catch (OConcurrentModificationException e) {
        Assert.fail("Should not failed here...");
      }
      Assert.assertEquals(vDocA_db1.field(NAME), "docA_v3");

      // Will throw OConcurrentModificationException
      database2.activateOnCurrentThread();
      database2.commit();
      Assert.fail("Should throw OConcurrentModificationException");
    } catch (OConcurrentModificationException e) {
      database2.rollback();
    }

    // Force reload all (to be sure it is not a cache problem)
    database1.activateOnCurrentThread();
    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");

    // docB should be in the last state : "docA_v3"
    ODocument vDocB_db2 = database2.load(vDocA_Rid);
    Assert.assertEquals(vDocB_db2.field(NAME), "docA_v3");

    database1.activateOnCurrentThread();
    database1.close();
    database2.activateOnCurrentThread();
    database2.close();
  }

  @Test
  public void test5CacheUpdatedMultipleDbs() {
    database1 = new ODatabaseDocumentTx(url).open("admin", "admin");

    // Create docA in db1
    database1.begin(TXTYPE.OPTIMISTIC);
    ODocument vDocA_db1 = database1.newInstance();
    vDocA_db1.field(NAME, "docA");
    database1.save(vDocA_db1, database1.getClusterNameById(database1.getDefaultClusterId()));
    database1.commit();

    // Keep the ID.
    ORID vDocA_Rid = vDocA_db1.getIdentity().copy();

    // Update docA in db2
    database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
    database2.begin(TXTYPE.OPTIMISTIC);
    ODocument vDocA_db2 = database2.load(vDocA_Rid);
    vDocA_db2.field(NAME, "docA_v2");
    database2.save(vDocA_db2);
    database2.commit();

    // Later... read docA with db1.
    database1.activateOnCurrentThread();
    database1.begin(TXTYPE.OPTIMISTIC);
    ODocument vDocA_db1_later = database1.load(vDocA_Rid, null, true);
    Assert.assertEquals(vDocA_db1_later.field(NAME), "docA_v2");
    database1.commit();

    database1.close();

    database2.activateOnCurrentThread();
    database2.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void checkVersionsInConnectedDocuments() {
    database = new ODatabaseDocumentTx(url).open("admin", "admin");
    database.begin();

    ODocument kim = new ODocument("Profile").field("name", "Kim").field("surname", "Bauer");
    ODocument teri = new ODocument("Profile").field("name", "Teri").field("surname", "Bauer");
    ODocument jack = new ODocument("Profile").field("name", "Jack").field("surname", "Bauer");

    ((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following"))
        .add(kim);
    ((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following"))
        .add(teri);
    ((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following"))
        .add(jack);

    jack.save();

    database.commit();

    database.close();
    database.open("admin", "admin");

    ODocument loadedJack = database.load(jack.getIdentity());

    int jackLastVersion = loadedJack.getVersion();
    database.begin();
    loadedJack.field("occupation", "agent");
    loadedJack.save();
    database.commit();
    Assert.assertTrue(jackLastVersion != loadedJack.getVersion());

    loadedJack = database.load(jack.getIdentity());
    Assert.assertTrue(jackLastVersion != loadedJack.getVersion());

    database.close();

    database.open("admin", "admin");
    loadedJack = database.load(jack.getIdentity());
    Assert.assertTrue(jackLastVersion != loadedJack.getVersion());
    database.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void createLinkInTx() {
    database = new ODatabaseDocumentTx(url).open("admin", "admin");

    OClass profile = database.getMetadata().getSchema().createClass("MyProfile", 1, null);
    OClass edge = database.getMetadata().getSchema().createClass("MyEdge", 1, null);
    profile
        .createProperty("name", OType.STRING)
        .setMin("3")
        .setMax("30")
        .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    profile.createProperty("surname", OType.STRING).setMin("3").setMax("30");
    profile.createProperty("in", OType.LINKSET, edge);
    profile.createProperty("out", OType.LINKSET, edge);
    edge.createProperty("in", OType.LINK, profile);
    edge.createProperty("out", OType.LINK, profile);

    database.begin();

    ODocument kim = new ODocument("MyProfile").field("name", "Kim").field("surname", "Bauer");
    ODocument teri = new ODocument("MyProfile").field("name", "Teri").field("surname", "Bauer");
    ODocument jack = new ODocument("MyProfile").field("name", "Jack").field("surname", "Bauer");

    ODocument myedge = new ODocument("MyEdge").field("in", kim).field("out", jack);
    myedge.save();
    ((HashSet<ODocument>) kim.field("out", new HashSet<ORID>()).field("out")).add(myedge);
    ((HashSet<ODocument>) jack.field("in", new HashSet<ORID>()).field("in")).add(myedge);

    jack.save();
    kim.save();
    teri.save();

    database.commit();

    database.close();

    database.open("admin", "admin");
    List<ODocument> result =
        database.command(new OSQLSynchQuery<ODocument>("select from MyProfile ")).execute();

    Assert.assertTrue(result.size() != 0);

    database.close();
  }

  @SuppressWarnings("unchecked")
  @Test
  public void loadRecordTest() {
    database = new ODatabaseDocumentTx(url).open("admin", "admin");
    database.begin();

    ODocument kim = new ODocument("Profile").field("name", "Kim").field("surname", "Bauer");
    ODocument teri = new ODocument("Profile").field("name", "Teri").field("surname", "Bauer");
    ODocument jack = new ODocument("Profile").field("name", "Jack").field("surname", "Bauer");
    ODocument chloe = new ODocument("Profile").field("name", "Chloe").field("surname", "O'Brien");

    ((HashSet<ODocument>) jack.field("following", new HashSet<ODocument>()).field("following"))
        .add(kim);
    ((HashSet<ODocument>) kim.field("following", new HashSet<ODocument>()).field("following"))
        .add(teri);
    ((HashSet<ODocument>) teri.field("following", new HashSet<ODocument>()).field("following"))
        .add(jack);
    ((HashSet<ODocument>) teri.field("following")).add(kim);
    ((HashSet<ODocument>) chloe.field("following", new HashSet<ODocument>()).field("following"))
        .add(jack);
    ((HashSet<ODocument>) chloe.field("following")).add(teri);
    ((HashSet<ODocument>) chloe.field("following")).add(kim);

    int profileClusterId = database.getClusterIdByName("Profile");

    jack.save();
    kim.save();
    teri.save();
    chloe.save();

    database.commit();

    Assert.assertEquals(jack.getIdentity().getClusterId(), profileClusterId);
    Assert.assertEquals(kim.getIdentity().getClusterId(), profileClusterId);
    Assert.assertEquals(teri.getIdentity().getClusterId(), profileClusterId);
    Assert.assertEquals(chloe.getIdentity().getClusterId(), profileClusterId);

    database.close();
    database.open("admin", "admin");

    ODocument loadedChloe = database.load(chloe.getIdentity());

    database.close();
  }

  @Test
  public void testTransactionPopulateDelete() {
    database = new ODatabaseDocumentTx(url).open("admin", "admin");
    if (!database.getMetadata().getSchema().existsClass("MyFruit")) {
      OClass fruitClass = database.getMetadata().getSchema().createClass("MyFruit");
      fruitClass.createProperty("name", OType.STRING);
      fruitClass.createProperty("color", OType.STRING);
      fruitClass.createProperty("flavor", OType.STRING);

      database
          .getMetadata()
          .getSchema()
          .getClass("MyFruit")
          .getProperty("name")
          .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      database
          .getMetadata()
          .getSchema()
          .getClass("MyFruit")
          .getProperty("color")
          .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
      database
          .getMetadata()
          .getSchema()
          .getClass("MyFruit")
          .getProperty("flavor")
          .createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    }
    database.close();

    database.open("admin", "admin");
    int chunkSize = getEnvironment() == DatabaseAbstractTest.ENV.DEV ? 10 : 500;
    for (int initialValue = 0; initialValue < 10; initialValue++) {
      // System.out.println("initialValue = " + initialValue);
      Assert.assertEquals(database.countClusterElements("MyFruit"), 0);

      System.out.println(
          "[testTransactionPopulateDelete] Populating chunk "
              + initialValue
              + "... (chunk="
              + chunkSize
              + ")");

      // do insert
      Vector<ODocument> v = new Vector<ODocument>();
      database.begin();
      for (int i = initialValue * chunkSize; i < (initialValue * chunkSize) + chunkSize; i++) {
        ODocument d =
            new ODocument("MyFruit")
                .field("name", "" + i)
                .field("color", "FOO")
                .field("flavor", "BAR" + i);
        d.save();
        v.addElement(d);
      }

      System.out.println(
          "[testTransactionPopulateDelete] Committing chunk " + initialValue + "...");

      // System.out.println("populate commit");
      database.commit();

      System.out.println(
          "[testTransactionPopulateDelete] Committed chunk "
              + initialValue
              + ", starting to delete all the new entries ("
              + v.size()
              + ")...");

      // do delete
      database.begin();
      // System.out.println("vector size = " + v.size());
      for (int i = 0; i < v.size(); i++) {
        database.delete(v.elementAt(i));
      }
      // System.out.println("delete commit");
      database.commit();

      System.out.println("[testTransactionPopulateDelete] Deleted executed successfully");

      Assert.assertEquals(database.countClusterElements("MyFruit"), 0);
    }

    System.out.println("[testTransactionPopulateDelete] End of the test");

    database.close();
  }

  @Test
  public void testConsistencyOnDelete() {
    final OrientGraph graph = new OrientGraph(url);

    if (graph.getVertexType("Foo") == null) graph.createVertexType("Foo");

    try {
      // Step 1
      // Create several foo's
      graph.addVertex("class:Foo", "address", "test1");
      graph.addVertex("class:Foo", "address", "test2");
      graph.addVertex("class:Foo", "address", "test3");
      graph.commit();

      // just show what is there
      List<ODocument> result =
          graph.getRawGraph().query(new OSQLSynchQuery<ODocument>("select * from Foo"));

      // for (ODocument d : result) {
      // System.out.println("Vertex: " + d);
      // }

      // remove those foos in a transaction
      // Step 3a
      result =
          graph
              .getRawGraph()
              .query(new OSQLSynchQuery<ODocument>("select * from Foo where address = 'test1'"));
      Assert.assertEquals(result.size(), 1);
      // Step 4a
      graph.removeVertex(graph.getVertex(result.get(0)));

      // Step 3b
      result =
          graph
              .getRawGraph()
              .query(new OSQLSynchQuery<ODocument>("select * from Foo where address = 'test2'"));
      Assert.assertEquals(result.size(), 1);
      // Step 4b
      graph.removeVertex(graph.getVertex(result.get(0)));

      // Step 3c
      result =
          graph
              .getRawGraph()
              .query(new OSQLSynchQuery<ODocument>("select * from Foo where address = 'test3'"));
      Assert.assertEquals(result.size(), 1);
      // Step 4c
      graph.removeVertex(graph.getVertex(result.get(0)));

      // Step 6
      graph.commit();

      // just show what is there
      result = graph.getRawGraph().query(new OSQLSynchQuery<ODocument>("select * from Foo"));

      // for (ODocument d : result) {
      // System.out.println("Vertex: " + d);
      // }

    } finally {
      graph.shutdown();
    }
  }

  @Test
  public void deletesWithinTransactionArentWorking() throws IOException {
    OrientGraph graph = new OrientGraph(url);
    graph.setUseLightweightEdges(false);
    try {
      if (graph.getVertexType("Foo") == null) graph.createVertexType("Foo");
      if (graph.getVertexType("Bar") == null) graph.createVertexType("Bar");
      if (graph.getVertexType("Sees") == null) graph.createEdgeType("Sees");

      // Commenting out the transaction will result in the test succeeding.
      ODocument foo = graph.addVertex("class:Foo", "prop", "test1").getRecord();

      // Comment out these two lines and the test will succeed. The issue appears to be related to
      // an edge
      // connecting a deleted vertex during a transaction
      ODocument bar = graph.addVertex("class:Bar", "prop", "test1").getRecord();
      ODocument sees =
          graph.addEdge(null, graph.getVertex(foo), graph.getVertex(bar), "Sees").getRecord();
      graph.commit();

      List<ODocument> foos = graph.getRawGraph().query(new OSQLSynchQuery("select * from Foo"));
      Assert.assertEquals(foos.size(), 1);

      graph.removeVertex(graph.getVertex(foos.get(0)));
    } finally {
      graph.shutdown();
    }
  }

  public void TransactionRollbackConstistencyTest() {
    // System.out.println("**************************TransactionRollbackConsistencyTest***************************************");

    database = new ODatabaseDocumentTx(url).open("admin", "admin");
    OClass vertexClass = database.getMetadata().getSchema().createClass("TRVertex");
    OClass edgeClass = database.getMetadata().getSchema().createClass("TREdge");
    vertexClass.createProperty("in", OType.LINKSET, edgeClass);
    vertexClass.createProperty("out", OType.LINKSET, edgeClass);
    edgeClass.createProperty("in", OType.LINK, vertexClass);
    edgeClass.createProperty("out", OType.LINK, vertexClass);

    OClass personClass = database.getMetadata().getSchema().createClass("TRPerson", vertexClass);
    personClass.createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty("surname", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
    personClass.createProperty("version", OType.INTEGER);

    database.close();

    final int cnt = 4;

    database.open("admin", "admin");
    database.begin();
    Vector inserted = new Vector();

    for (int i = 0; i < cnt; i++) {
      ODocument person = new ODocument("TRPerson");
      person.field("name", Character.toString((char) ('A' + i)));
      person.field("surname", Character.toString((char) ('A' + (i % 3))));
      person.field("myversion", 0);
      person.field("in", new HashSet<ODocument>());
      person.field("out", new HashSet<ODocument>());

      if (i >= 1) {
        ODocument edge = new ODocument("TREdge");
        edge.field("in", person.getIdentity());
        edge.field("out", inserted.elementAt(i - 1));
        ((Set<ODocument>) person.field("out")).add(edge);
        ((Set<ODocument>) ((ODocument) inserted.elementAt(i - 1)).field("in")).add(edge);
        edge.save();
      }
      inserted.add(person);
      person.save();
    }
    database.commit();

    final List<ODocument> result1 =
        database.command(new OCommandSQL("select from TRPerson")).execute();
    Assert.assertNotNull(result1);
    Assert.assertEquals(result1.size(), cnt);
    // System.out.println("Before transaction commit");
    // for (ODocument d : result1)
    // System.out.println(d);

    try {
      database.begin();
      Vector inserted2 = new Vector();

      for (int i = 0; i < cnt; i++) {
        ODocument person = new ODocument("TRPerson");
        person.field("name", Character.toString((char) ('a' + i)));
        person.field("surname", Character.toString((char) ('a' + (i % 3))));
        person.field("myversion", 0);
        person.field("in", new HashSet<ODocument>());
        person.field("out", new HashSet<ODocument>());

        if (i >= 1) {
          ODocument edge = new ODocument("TREdge");
          edge.field("in", person.getIdentity());
          edge.field("out", inserted2.elementAt(i - 1));
          ((Set<ODocument>) person.field("out")).add(edge);
          ((Set<ODocument>) ((ODocument) inserted2.elementAt(i - 1)).field("in")).add(edge);
          edge.save();
        }
        inserted2.add(person);
        person.save();
      }

      for (int i = 0; i < cnt; i++) {
        if (i != cnt - 1) {
          ((ODocument) inserted.elementAt(i)).field("myversion", 2);
          ((ODocument) inserted.elementAt(i)).save();
        }
      }

      ((ODocument) inserted.elementAt(cnt - 1)).delete();
      ORecordInternal.setVersion(((ODocument) inserted.elementAt(cnt - 2)), 0);
      ((ODocument) inserted.elementAt(cnt - 2)).save();
      database.commit();
      Assert.assertTrue(false);
    } catch (OConcurrentModificationException e) {
      Assert.assertTrue(true);
      database.rollback();
    }

    final List<ODocument> result2 =
        database.command(new OCommandSQL("select from TRPerson")).execute();
    Assert.assertNotNull(result2);
    // System.out.println("After transaction commit failure/rollback");
    // for (ODocument d : result2)
    // System.out.println(d);
    Assert.assertEquals(result2.size(), cnt);

    // System.out.println("**************************TransactionRollbackConstistencyTest***************************************");
  }

  @Test
  public void testQueryIsolation() {
    OrientGraph graph = new OrientGraph(url);
    try {
      graph.addVertex(null, "purpose", "testQueryIsolation");

      if (!url.startsWith("remote")) {
        List<OIdentifiable> result =
            graph
                .getRawGraph()
                .query(
                    new OSQLSynchQuery<Object>(
                        "select from V where purpose = 'testQueryIsolation'"));
        Assert.assertEquals(result.size(), 1);
      }

      graph.commit();

      List<OIdentifiable> result =
          graph
              .getRawGraph()
              .query(
                  new OSQLSynchQuery<Object>("select from V where purpose = 'testQueryIsolation'"));
      Assert.assertEquals(result.size(), 1);

    } finally {
      graph.shutdown();
    }
  }

  /**
   * When calling .remove(o) on a collection, the row corresponding to o is deleted and not restored
   * when the transaction is rolled back.
   *
   * <p>Commented code after data model change to work around this problem.
   */
  @SuppressWarnings("unused")
  @Test
  public void testRollbackWithRemove() {
    // check if the database exists and clean before running tests
    OObjectDatabaseTx database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");

    try {
      Account account = new Account();
      account.setName("John Grisham");
      account = database.save(account);

      Address address1 = new Address();
      address1.setStreet("Mulholland drive");

      Address address2 = new Address();
      address2.setStreet("Via Veneto");

      List<Address> addresses = new ArrayList<Address>();
      addresses.add(address1);
      addresses.add(address2);
      account.setAddresses(addresses);

      account = database.save(account);

      database.commit();

      String originalName = account.getName();

      database.begin(TXTYPE.OPTIMISTIC);

      Assert.assertEquals(account.getAddresses().size(), 2);
      account
          .getAddresses()
          .remove(
              1); // delete one of the objects in the Books collection to see how rollback behaves
      Assert.assertEquals(account.getAddresses().size(), 1);
      account.setName("New Name"); // change an attribute to see if the change is rolled back
      account = database.save(account);

      Assert.assertEquals(
          account.getAddresses().size(),
          1); // before rollback this is fine because one of the books was removed

      database.rollback(); // rollback the transaction

      account =
          database.reload(account, true); // ignore cache, get a copy of author from the datastore
      Assert.assertEquals(
          account.getAddresses().size(), 2); // this is fine, author still linked to 2 books
      Assert.assertEquals(account.getName(), originalName); // name is restored

      int bookCount = 0;
      for (Address b : database.browseClass(Address.class)) {
        if (b.getStreet().equals("Mulholland drive") || b.getStreet().equals("Via Veneto"))
          bookCount++;
      }
      Assert.assertEquals(bookCount, 2); // this fails, only 1 entry in the datastore :(
    } finally {
      database.close();
    }
  }

  public void testTransactionsCache() throws Exception {
    OObjectDatabaseTx database = new OObjectDatabaseTx(url);
    database.open("admin", "admin");

    try {
      Assert.assertFalse(database.getTransaction().isActive());
      OSchema schema = database.getMetadata().getSchema();
      OClass classA = schema.createClass("TransA");
      classA.createProperty("name", OType.STRING);
      ODocument doc = new ODocument(classA);
      doc.field("name", "test1");
      doc.save();
      ORID orid = doc.getIdentity();
      database.begin();
      Assert.assertTrue(database.getTransaction().isActive());
      doc = orid.getRecord();
      Assert.assertEquals("test1", doc.field("name"));
      doc.field("name", "test2");
      doc = orid.getRecord();
      Assert.assertEquals("test2", doc.field("name"));
      // There is NO SAVE!
      database.commit();

      doc = orid.getRecord();
      Assert.assertEquals("test1", doc.field("name"));

    } finally {
      database.close();
    }
  }
}
