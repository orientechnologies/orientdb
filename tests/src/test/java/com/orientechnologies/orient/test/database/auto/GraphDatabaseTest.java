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
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.graph.OGraphDatabasePool;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.tx.OTransaction;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;

@Test
public class GraphDatabaseTest {
  private OGraphDatabase database;
  private String         url;

  @Parameters(value = "url")
  public GraphDatabaseTest(String iURL) {
    database = new OGraphDatabase(iURL);
    url = iURL;
  }

  @Test
  public void testPool() throws IOException {
    final OGraphDatabase[] dbs = new OGraphDatabase[OGraphDatabasePool.global().getMaxSize()];

    for (int i = 0; i < 10; ++i) {
      for (int db = 0; db < dbs.length; ++db)
        dbs[db] = OGraphDatabasePool.global().acquire(url, "admin", "admin");
      for (int db = 0; db < dbs.length; ++db)
        dbs[db].close();
    }
  }

  @BeforeMethod
  public void init() {
    database.open("admin", "admin");
  }

  @AfterMethod
  public void deinit() {
    database.close();
  }

  @Test
  public void alterDb() {
    database.command(new OCommandSQL("alter database type graph")).execute();
  }

  @Test(dependsOnMethods = "alterDb")
  public void populate() {
    OClass vehicleClass = database.createVertexType("GraphVehicle");
    database.createVertexType("GraphCar", vehicleClass);
    database.createVertexType("GraphMotocycle", "GraphVehicle");

    ODocument carNode = (ODocument) database.createVertex("GraphCar").field("brand", "Hyundai").field("model", "Coupe")
        .field("year", 2003).save();
    ODocument motoNode = (ODocument) database.createVertex("GraphMotocycle").field("brand", "Yamaha").field("model", "X-City 250")
        .field("year", 2009).save();

    database.createEdge(carNode, motoNode).save();

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select from GraphVehicle"));
    Assert.assertEquals(result.size(), 2);
    for (ODocument v : result) {
      Assert.assertTrue(v.getSchemaClass().isSubClassOf(vehicleClass));
    }

    database.close();

    database.open("admin", "admin");

    database.getMetadata().getSchema().reload();

    result = database.query(new OSQLSynchQuery<ODocument>("select from GraphVehicle"));
    Assert.assertEquals(result.size(), 2);

    ODocument edge1 = null;
    ODocument edge2 = null;

    for (ODocument v : result) {
      Assert.assertTrue(v.getSchemaClass().isSubClassOf("GraphVehicle"));

      if (v.getClassName().equals("GraphCar")) {
        Assert.assertEquals(database.getOutEdges(v).size(), 1);
        edge1 = (ODocument) database.getOutEdges(v).iterator().next();
      } else {
        Assert.assertEquals(database.getInEdges(v).size(), 1);
        edge2 = (ODocument) database.getInEdges(v).iterator().next();
      }
    }

    Assert.assertEquals(edge1, edge2);

  }

  @Test(dependsOnMethods = "populate")
  public void testSQLAgainstGraph() {
    ODocument tom = (ODocument) database.createVertex().field("name", "Tom").save();
    ODocument ferrari = (ODocument) database.createVertex("GraphCar").field("brand", "Ferrari").save();
    ODocument maserati = (ODocument) database.createVertex("GraphCar").field("brand", "Maserati").save();
    ODocument porsche = (ODocument) database.createVertex("GraphCar").field("brand", "Porsche").save();
    database.createEdge(tom, ferrari).field("label", "drives").save();
    database.createEdge(tom, maserati).field("label", "drives").save();
    database.createEdge(tom, porsche).field("label", "owns").save();

    Assert.assertNotNull(database.getOutEdges(tom, "drives"));
    Assert.assertFalse(database.getOutEdges(tom, "drives").isEmpty());

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(
        "select out_[in.@class = 'GraphCar'].in_ from V where name = 'Tom'"));
    Assert.assertEquals(result.size(), 1);

    result = database.query(new OSQLSynchQuery<ODocument>(
        "select out_[label='drives'][in.brand = 'Ferrari'].in_ from V where name = 'Tom'"));
    Assert.assertEquals(result.size(), 1);

    result = database.query(new OSQLSynchQuery<ODocument>("select out_[in.brand = 'Ferrari'].in_ from V where name = 'Tom'"));
    Assert.assertEquals(result.size(), 1);
  }

  @Test
  public void testDictionary() {
    ODocument rootNode = database.createVertex().field("id", 54254454);
    database.setRoot("test123", rootNode);
    rootNode.save();

    database.close();
    database.open("admin", "admin");

    ODocument secroot = database.getRoot("test123");
    Assert.assertEquals(secroot.getIdentity(), rootNode.getIdentity());
  }

  @Test
  public void testSubVertexQuery() {
    database.createVertexType("newV").createProperty("f_int", OType.INTEGER).createIndex(OClass.INDEX_TYPE.UNIQUE);
    database.getMetadata().getSchema().save();

    database.createVertex("newV").field("f_int", 2).save();
    database.createVertex("newV").field("f_int", 1).save();
    database.createVertex("newV").field("f_int", 3).save();

    // query 1
    String q = "select * from V where f_int between 0 and 10";
    List<ODocument> resB = database.query(new OSQLSynchQuery<ODocument>(q));
    System.out.println(q + ": ");
    for (OIdentifiable v : resB) {
      System.out.println(v);
    }

    // query 2
    q = "select * from newV where f_int between 0 and 10";
    List<ODocument> resB2 = database.query(new OSQLSynchQuery<ODocument>(q));
    System.out.println(q + ": ");
    for (OIdentifiable v : resB2) {
      System.out.println(v);
    }
  }

  public void testNotDuplicatedIndexTxChanges() throws IOException {
    OClass oc = database.getVertexType("vertexA");
    if (oc == null)
      oc = database.createVertexType("vertexA");
    if (!oc.existsProperty("name"))
      oc.createProperty("name", OType.STRING);

    if (oc.getClassIndex("vertexA_name_idx") == null)
      oc.createIndex("vertexA_name_idx", OClass.INDEX_TYPE.UNIQUE, "name");

    // FIRST: create a couple of records
    ODocument docA = database.createVertex("vertexA");
    docA.field("name", "myKey");
    database.save(docA);

    ODocument docB = database.createVertex("vertexA");
    docA.field("name", "anotherKey");
    database.save(docB);

    database.begin();
    database.delete(docB);
    database.delete(docA);
    ODocument docKey = database.createVertex("vertexA");
    docKey.field("name", "myKey");
    database.save(docKey);
    database.commit();
  }

  public void testAutoEdge() throws IOException {
    ODocument docA = database.createVertex();
    docA.field("name", "selfEdgeTest");
    database.createEdge(docA, docA).save();

    docA.reload();
  }

  public void testNewVertexAndEdgesWithFieldsInOneShoot() throws IOException {
    ODocument docA = database.createVertex(null, "field1", "value1", "field2", "value2");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("field1", "value1");
    map.put("field2", "value2");
    ODocument docB = database.createVertex(null, map);

    ODocument docC = database.createEdge(docA, docA, null, "edgeF1", "edgeV2").save();

    Assert.assertEquals(docA.field("field1"), "value1");
    Assert.assertEquals(docA.field("field2"), "value2");

    Assert.assertEquals(docB.field("field1"), "value1");
    Assert.assertEquals(docB.field("field2"), "value2");

    Assert.assertEquals(docC.field("edgeF1"), "edgeV2");
  }

  public void testEdgesIterationInTX() {
    database.createVertexType("vertexAA");
    database.createVertexType("vertexBB");
    database.createEdgeType("edgeAB");

    ODocument vertexA = (ODocument) database.createVertex("vertexAA").field("address", "testing").save();

    for (int i = 0; i < 18; ++i) {
      ODocument vertexB = (ODocument) database.createVertex("vertexBB").field("address", "test" + i).save();
      database.begin(OTransaction.TXTYPE.OPTIMISTIC);
      database.createEdge(vertexB.getIdentity(), vertexA.getIdentity(), "edgeAB").save();
      database.commit();
    }

    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select * from vertexAA"));
    for (ODocument d : result) {
      Set<OIdentifiable> edges = database.getInEdges(d);
      for (OIdentifiable e : edges) {
        System.out.println("In Edge: " + e);
      }
    }
  }

  /**
   * @author bill@tobecker.com
   */
  public void testTxField() {
    if (database.getVertexType("PublicCert") == null)
      database.createVertexType("PublicCert");

    // Step 1
    // create a public cert with some field set
    ODocument publicCert = (ODocument) database.createVertex("PublicCert").field("address", "drevil@myco.mn.us").save();

    // Step 2
    // update the public cert field in transaction
    database.begin(TXTYPE.OPTIMISTIC);
    publicCert.field("address", "newaddress@myco.mn.us").save();
    database.commit();

    // Step 3
    // try transaction with a rollback
    database.begin(TXTYPE.OPTIMISTIC);
    database.createVertex("PublicCert").field("address", "iagor@myco.mn.us").save();
    database.rollback();

    // Step 4
    // just show what is there
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>("select * from PublicCert"));

    for (ODocument d : result) {
      System.out.println("(-1) Vertex: " + d);
    }

    // Step 5
    // try deleting all the stuff
    database.command(new OCommandSQL("delete from PublicCert")).execute();
  }

  @Test(dependsOnMethods = "populate")
  public void testEdgeWithRID() {
    database.declareIntent(new OIntentMassiveInsert());

    ODocument a = database.createVertex().field("label", "a");
    a.save();
    ODocument b = database.createVertex().field("label", "b");
    b.save();
    ODocument c = database.createVertex().field("label", "c");
    c.save();

    database.createEdge(a.getIdentity(), b.getIdentity()).save();
    database.createEdge(a.getIdentity(), c.getIdentity()).save();

    a.reload();
    // Assert.assertEquals(database.getOutEdges(a).size(), 2);
  }

  @Test(dependsOnMethods = "populate")
  public void testEdgeCreationIn2Steps() {
    // add source
    ODocument sourceDoc = database.createVertex();
    sourceDoc.field("name", "MyTest", OType.STRING);
    sourceDoc.save();

    // add first office
    ODocument office1Doc = database.createVertex();
    office1Doc.field("name", "office1", OType.STRING);
    office1Doc.save();

    List<ODocument> source1 = database.query(new OSQLSynchQuery<ODocument>("select * from V where name = 'MyTest'"));
    for (int i = 0; i < source1.size(); i++)
      database.createEdge(source1.get(i), office1Doc).field("label", "office", OType.STRING).save();

    String query11 = "select out_[label='office'].size() from V where name = 'MyTest'";
    List<ODocument> officesDoc11 = database.query(new OSQLSynchQuery<ODocument>(query11));
    System.out.println(officesDoc11);

    // add second office
    ODocument office2Doc = database.createVertex();
    office2Doc.field("name", "office2", OType.STRING);
    office2Doc.save();

    List<ODocument> source2 = database.query(new OSQLSynchQuery<ODocument>("select * from V where name = 'MyTest'"));
    for (int i = 0; i < source2.size(); i++)
      database.createEdge(source2.get(i), office2Doc).field("label", "office", OType.STRING).save();

    String query21 = "select out_[label='office'].size() from V where name = 'MyTest'";
    List<ODocument> officesDoc21 = database.query(new OSQLSynchQuery<ODocument>(query21));
    System.out.println(officesDoc21);
  }

  @Test
  public void saveEdges() {
    database.declareIntent(new OIntentMassiveInsert());

    ODocument v = database.createVertex();
    v.field("name", "superNode");

    long insertBegin = System.currentTimeMillis();

    long begin = insertBegin;
    Set<Integer> identities = new HashSet<Integer>(1000);
    for (int i = 1; i <= 1000; ++i) {
      database.createEdge(v, database.createVertex().field("id", i)).save();
      Assert.assertTrue(identities.add(i));
      if (i % 100 == 0) {
        final long now = System.currentTimeMillis();
        System.out.printf("\nInserted %d edges, elapsed %d ms. v.out=%d", i, now - begin, ((Set<?>) v.field("out_")).size());
        begin = System.currentTimeMillis();
      }
    }
    Assert.assertEquals(identities.size(), 1000);

    int originalEdges = ((Set<?>) v.field("out_")).size();
    System.out.println("Edge count (Original instance): " + originalEdges);

    ODocument x = database.load(v.getIdentity());
    int loadedEdges = ((Set<?>) x.field("out_")).size();
    System.out.println("Edge count (Loaded instance): " + loadedEdges);

    Assert.assertEquals(originalEdges, loadedEdges);

    long now = System.currentTimeMillis();
    System.out.printf("\nInsertion completed in %dms. DB edges %d, DB vertices %d", now - insertBegin, database.countEdges(),
        database.countVertexes());

    int i = 1;
    for (OIdentifiable e : database.getOutEdges(v)) {
      Integer currentIdentity = database.getInVertex(e).field("id");
      Assert.assertTrue(identities.contains(currentIdentity));
      Assert.assertTrue(identities.remove(currentIdentity));
      if (i % 100 == 0) {
        now = System.currentTimeMillis();
        System.out.printf("\nRead %d edges and %d vertices, elapsed %d ms", i, i, now - begin);
        begin = System.currentTimeMillis();
      }
      i++;
    }
    Assert.assertTrue(identities.isEmpty());
    database.declareIntent(null);
  }

  @Test
  public void sqlInsertIntoVertexes() {
    List<OIdentifiable> vertices = database.command(new OCommandSQL("select from V limit 2")).execute();
    Assert.assertEquals(vertices.size(), 2);

    OIdentifiable v1 = ((ODocument) vertices.get(0)).reload();
    OIdentifiable v2 = ((ODocument) vertices.get(1)).reload();

    final int v1Edges = database.getOutEdges(v1).size();
    final int v2Edges = database.getInEdges(v2).size();

    ODocument e = database.command(new OCommandSQL("insert into E SET out_ = ?, in_ = ?")).execute(v1, v2);
    database.command(new OCommandSQL("update " + v1.getIdentity() + " ADD out_ = " + e.getIdentity())).execute();
    database.command(new OCommandSQL("update " + v2.getIdentity() + " ADD in_ = " + e.getIdentity())).execute();

    ODocument doc1 = ((ODocument) v1.getRecord().reload());
    ODocument doc2 = ((ODocument) v2.getRecord().reload());
    Assert.assertEquals(database.getOutEdges(doc1).size(), v1Edges + 1);
    Assert.assertEquals(database.getInEdges(doc2).size(), v2Edges + 1);
  }

  @Test
  public void sqlNestedQueries() {
    ODocument sourceDoc1 = database.createVertex().field("driver", "John", OType.STRING).save();
    ODocument targetDoc1 = database.createVertex().field("car", "ford", OType.STRING).save();
    ODocument targetDoc2 = database.createVertex().field("car", "audi", OType.STRING).save();

    database.createEdge(sourceDoc1, targetDoc1).field("color", "red", OType.STRING).field("action", "owns", OType.STRING).save();
    database.createEdge(sourceDoc1, targetDoc2).field("color", "red", OType.STRING).field("action", "wants", OType.STRING).save();

    String query1 = "select driver from V where out_.in.car in 'ford'";
    List<ODocument> result = database.query(new OSQLSynchQuery<ODocument>(query1));
    Assert.assertEquals(result.size(), 1);

    String query2 = "select driver from V where out_[color='red'].in.car in 'ford'";
    result = database.query(new OSQLSynchQuery<ODocument>(query2));
    Assert.assertEquals(result.size(), 1);

    String query3 = "select driver from V where out_[action='owns'].in.car in 'ford'";
    result = database.query(new OSQLSynchQuery<ODocument>(query3));
    Assert.assertEquals(result.size(), 1);

    String query4 = "select driver from V where out_[color='red'][action='owns'].in.car in 'ford'";
    result = database.query(new OSQLSynchQuery<ODocument>(query4));
    Assert.assertEquals(result.size(), 1);

    database.removeVertex(sourceDoc1);
    targetDoc1.reload();
    database.removeVertex(targetDoc1);
    targetDoc2.reload();
    database.removeVertex(targetDoc2);
  }

  @SuppressWarnings("unchecked")
  public void nestedQuery() {
    ODocument countryDoc1 = database.createVertex().field("name", "UK").field("area", "Europe").field("code", "2").save();
    ODocument cityDoc1 = database.createVertex().field("name", "leicester").field("lat", "52.64640").field("long", "-1.13159")
        .save();
    ODocument cityDoc2 = database.createVertex().field("name", "manchester").field("lat", "53.47497").field("long", "-2.25769")
        .save();
    database.createEdge(countryDoc1, cityDoc1).field("label", "owns").save();
    database.createEdge(countryDoc1, cityDoc2).field("label", "owns").save();

    String subquery = "select out_[label='owns'].in from V where name = 'UK'";
    List<OIdentifiable> result = database.query(new OSQLSynchQuery<ODocument>(subquery));

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((Collection<ODocument>) ((ODocument) result.get(0)).field("out_")).size(), 2);

    subquery = "select expand(out_[label='owns'].in) from V where name = 'UK'";
    result = database.query(new OSQLSynchQuery<ODocument>(subquery));

    Assert.assertEquals(result.size(), 2);
    for (int i = 0; i < result.size(); i++) {
      System.out.println("uno: " + result.get(i));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("lat"));
    }

    String query = "select name, lat, long, distance(lat,long,51.5,0.08) as distance from (select expand(out_[label='owns'].in) from V where name = 'UK') order by distance";
    result = database.query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    for (int i = 0; i < result.size(); i++) {
      System.out.println("dos: " + result.get(i));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("lat"));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("distance"));
    }
  }

  public void testexpandBlankDatabase() throws IOException {
    String iUrl = url;
    iUrl.replace("\\", "/");
    if (iUrl.endsWith("/"))
      iUrl = iUrl.substring(0, iUrl.length() - 1);
    if (iUrl.contains("/")) {
      iUrl = iUrl.substring(0, iUrl.lastIndexOf("/") + 1) + "expandTest";
    } else {
      iUrl = iUrl.substring(0, iUrl.indexOf(":") + 1) + "expandTest";
    }
    ODatabaseDocument db = new ODatabaseDocumentTx(iUrl);

    ODatabaseHelper.createDatabase(db, iUrl, "plocal");
    db.close();
    OGraphDatabase database = new OGraphDatabase(iUrl);
    database.open("admin", "admin");

    ODocument playerDoc = database.createVertex().field("surname", "Torres").save();
    ODocument teamDoc = database.createVertex().field("team", "Chelsea").save();
    database.createEdge(playerDoc, teamDoc).field("label", "player").save();

    String query = "select expand(out_[label='player'].in) from V where surname = 'Torres'";
    List<OIdentifiable> result = database.query(new OSQLSynchQuery<ODocument>(query));
    for (int i = 0; i < result.size(); i++) {
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("team"));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).field("team").equals("Chelsea"));
    }
    database.removeVertex(playerDoc);
    database.removeVertex(teamDoc);

    database.close();
    ODatabaseHelper.deleteDatabase(database, "plocal");
  }

  //
  // public void testSQLManagementOfUnderlyingDocumentsInGraphs() {
  // Object result;
  //
  // result = database.command(new OCommandSQL("create class V1 extends V")).execute();
  // result = database.command(new OCommandSQL("create class E1 extends E")).execute();
  //
  // OIdentifiable v1 = database.command(new OCommandSQL("create vertex V1 set name = 'madeInSqlLand'")).execute();
  // OIdentifiable v2 = database.command(new OCommandSQL("create vertex V1 set name = 'madeInSqlLand'")).execute();
  // OIdentifiable v3 = database.command(new OCommandSQL("create vertex V1 set name = 'madeInSqlLand'")).execute();
  // List<OIdentifiable> e1 = database.command(
  // new OCommandSQL("create edge E1 from " + v1.getIdentity() + " to " + v2.getIdentity() + " set name = 'wow' ")).execute();
  // List<OIdentifiable> e2 = database.command(
  // new OCommandSQL("create edge E1 from " + v1.getIdentity() + " to " + v3.getIdentity() + " set name = 'wow' ")).execute();
  //
  // result = database.command(
  // new OCommandSQL("delete edge from " + v1.getIdentity() + " to " + v2.getIdentity() + " where  name = 'wow'")).execute();
  // Assert.assertEquals(result, 1);
  // result = database.command(new OCommandSQL("delete edge where name = 'wow'")).execute();
  // Assert.assertEquals(result, 1);
  //
  // result = database.command(new OCommandSQL("delete from V1 where @rid = ?")).execute(v2);
  // Assert.assertEquals(result, 1);
  // result = database.command(new OCommandSQL("delete from V1 where @rid = ?")).execute(v3);
  // Assert.assertEquals(result, 1);
  //
  // result = database.command(new OCommandSQL("create property V1.ctime DATETIME")).execute();
  // // result = database.command(new OCommandSQL("update V1 set ctime=sysdate() where name = 'madeInSqlLand'")).execute();
  //
  // result = database.command(new OCommandSQL("drop class V1")).execute();
  // result = database.command(new OCommandSQL("drop class E1")).execute();
  // }

  @Test
  public void testTransactionNative() {
    ODocument a = null;
    try {
      database.begin(OTransaction.TXTYPE.OPTIMISTIC);

      a = database.createVertex().save();

      database.commit();
    } catch (Exception e) {
      database.rollback();
      e.printStackTrace();
    }

    try {
      database.begin(OTransaction.TXTYPE.OPTIMISTIC);

      for (int i = 0; i < 100; ++i) {
        database.createEdge(database.createVertex().save(), a.save()).save();
      }

      database.commit();
    } catch (Exception e) {
      database.rollback();
      e.printStackTrace();
    }

    try {
      database.begin(OTransaction.TXTYPE.OPTIMISTIC);

      database.createEdge(database.createVertex().save(), a.save()).save();

      database.commit();
    } catch (Exception e) {
      database.rollback();
      e.printStackTrace();
    }
  }
}
