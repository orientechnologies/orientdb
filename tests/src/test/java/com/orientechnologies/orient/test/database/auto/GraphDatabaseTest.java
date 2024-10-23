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

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class GraphDatabaseTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public GraphDatabaseTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  public void init() {}

  @AfterMethod
  public void deinit() {}

  @Test
  public void populate() {
    OClass vehicleClass = database.createVertexClass("GraphVehicle");
    database.createClass("GraphCar", vehicleClass.getName());
    database.createClass("GraphMotocycle", "GraphVehicle");

    OVertex carNode = database.newVertex("GraphCar");
    carNode.setProperty("brand", "Hyundai");
    carNode.setProperty("model", "Coupe");
    carNode.setProperty("year", 2003);
    database.save(carNode);
    OVertex motoNode = database.newVertex("GraphMotocycle");
    motoNode.setProperty("brand", "Yamaha");
    motoNode.setProperty("model", "X-City 250");
    motoNode.setProperty("year", 2009);
    database.save(motoNode);

    database.commit();
    database.newEdge(carNode, motoNode).save();

    List<OResult> result =
        database.query("select from GraphVehicle").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);
    for (OResult v : result) {
      Assert.assertTrue(v.getElement().get().getSchemaType().get().isSubClassOf(vehicleClass));
    }

    database.commit();
    database.close();
    reopendb("admin", "admin");

    database.setUseLightweightEdges(false);

    database.getMetadata().getSchema().reload();

    result = database.query("select from GraphVehicle").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    OEdge edge1 = null;
    OEdge edge2 = null;

    for (OResult v : result) {
      Assert.assertTrue(v.getElement().get().getSchemaType().get().isSubClassOf("GraphVehicle"));

      if (v.getElement().get().getSchemaType().isPresent()
          && v.getElement().get().getSchemaType().get().getName().equals("GraphCar")) {
        Assert.assertEquals(
            StreamSupport.stream(
                    ((OVertex) database.load(v.getIdentity().get()))
                        .getEdges(ODirection.OUT)
                        .spliterator(),
                    false)
                .count(),
            1);
        edge1 =
            (((OVertex) database.load(v.getIdentity().get())).getEdges(ODirection.OUT))
                .iterator()
                .next();
      } else {
        Assert.assertEquals(
            StreamSupport.stream(
                    ((OVertex) database.load(v.getIdentity().get()))
                        .getEdges(ODirection.IN)
                        .spliterator(),
                    false)
                .count(),
            1);
        edge2 =
            ((OVertex) database.load(v.getIdentity().get()))
                .getEdges(ODirection.IN)
                .iterator()
                .next();
      }
    }
    database.commit();

    Assert.assertEquals(edge1, edge2);
  }

  @Test(dependsOnMethods = "populate")
  public void testSQLAgainstGraph() {
    OVertex tom = database.newVertex();
    tom.setProperty("name", "Tom");
    database.save(tom);
    OVertex ferrari = database.newVertex("GraphCar");
    ferrari.setProperty("brand", "Ferrari");
    database.save(ferrari);

    OVertex maserati = database.newVertex("GraphCar");
    maserati.setProperty("brand", "Maserati");
    database.save(maserati);

    OVertex porsche = database.newVertex("GraphCar");
    porsche.setProperty("brand", "Porsche");
    database.save(porsche);

    database.createEdgeClass("drives");

    database.save(database.newEdge(tom, ferrari, "drives"));
    database.save(database.newEdge(tom, maserati, "drives"));
    database.save(database.newEdge(tom, porsche, "owns"));
    database.commit();

    Assert.assertNotNull(((OVertex) database.load(tom)).getEdges(ODirection.OUT, "drives"));
    Assert.assertEquals(
        StreamSupport.stream(
                ((OVertex) database.load(tom)).getEdges(ODirection.OUT, "drives").spliterator(),
                false)
            .count(),
        2);

    OResultSet result =
        database.query("select out_[in.@class = 'GraphCar'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);

    result =
        database.query(
            "select out_[label='drives'][in.brand = 'Ferrari'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);

    result = database.query("select out_[in.brand = 'Ferrari'].in_ from V where name = 'Tom'");
    Assert.assertEquals(result.stream().count(), 1);
  }

  public void testNotDuplicatedIndexTxChanges() throws IOException {
    database.commit();
    OClass oc = database.getClass("vertexA");
    if (oc == null) oc = database.createVertexClass("vertexA");
    if (!oc.existsProperty("name")) oc.createProperty("name", OType.STRING);

    if (oc.getClassIndex("vertexA_name_idx") == null)
      oc.createIndex("vertexA_name_idx", OClass.INDEX_TYPE.UNIQUE, "name");

    // FIRST: create a couple of records
    OVertex vertexA = database.newVertex("vertexA");
    vertexA.setProperty("name", "myKey");
    vertexA = database.save(vertexA);
    OVertex vertexB = database.newVertex("vertexA");
    vertexB.setProperty("name", "anotherKey");
    vertexB = database.save(vertexB);
    database.commit();

    database.delete(vertexB);
    database.delete(vertexA);
    OVertex vertexC = database.newVertex("vertexA");
    vertexC.setProperty("name", "myKey");
    database.commit();
  }

  public void testNewVertexAndEdgesWithFieldsInOneShoot() throws IOException {
    OVertex vertexA = database.newVertex();
    vertexA.setProperty("field1", "value1");
    vertexA.setProperty("field2", "value2");
    database.save(vertexA);

    OVertex vertexB = database.newVertex();
    vertexB.setProperty("field1", "value1");
    vertexB.setProperty("field2", "value2");
    database.save(vertexB);

    OEdge edgeC = database.newEdge(vertexA, vertexB, "E");
    edgeC.setProperty("edgeF1", "edgeV2");
    database.save(edgeC);

    database.commit();

    Assert.assertEquals(vertexA.getProperty("field1"), "value1");
    Assert.assertEquals(vertexA.getProperty("field2"), "value2");

    Assert.assertEquals(vertexB.getProperty("field1"), "value1");
    Assert.assertEquals(vertexB.getProperty("field2"), "value2");

    Assert.assertEquals(edgeC.getProperty("edgeF1"), "edgeV2");
  }

  @Test
  public void sqlNestedQueries() {
    OVertex vertex1 = database.newVertex();
    vertex1.setProperty("driver", "John");
    database.save(vertex1);
    OVertex vertex2 = database.newVertex();
    vertex2.setProperty("car", "ford");
    database.save(vertex2);
    OVertex targetVertex = database.newVertex();
    targetVertex.setProperty("car", "audi");
    database.save(targetVertex);

    OEdge edge = database.newEdge(vertex1, vertex2, "E");
    edge.setProperty("color", "red");
    edge.setProperty("action", "owns");
    database.save(edge);

    edge = database.newEdge(vertex1, targetVertex, "E");
    edge.setProperty("color", "red");
    edge.setProperty("action", "wants");
    database.save(edge);

    database.commit();

    String query1 = "select driver from V where out().car contains 'ford'";
    OResultSet result = database.query(query1);
    Assert.assertEquals(result.stream().count(), 1);

    String query2 = "select driver from V where outE()[color='red'].inV().car contains 'ford'";
    result = database.query(query2);
    Assert.assertEquals(result.stream().count(), 1);

    // TODO these tests are broken, they should test "contains" instead of "="
    String query3 = "select driver from V where outE()[action='owns'].inV().car = 'ford'";
    result = database.query(query3);
    Assert.assertEquals(result.stream().count(), 1);

    String query4 =
        "select driver from V where outE()[color='red'][action='owns'].inV().car = 'ford'";
    result = database.query(query4);
    Assert.assertEquals(result.stream().count(), 1);
  }

  public void nestedQuery() {
    OVertex countryVertex1 = database.newVertex();
    countryVertex1.setProperty("name", "UK");
    countryVertex1.setProperty("area", "Europe");
    countryVertex1.setProperty("code", "2");
    database.save(countryVertex1);

    OVertex cityVertex1 = database.newVertex();
    cityVertex1.setProperty("name", "leicester");
    cityVertex1.setProperty("lat", "52.64640");
    cityVertex1.setProperty("long", "-1.13159");

    database.save(cityVertex1);
    OVertex cityVertex2 = database.newVertex();
    cityVertex2.setProperty("name", "manchester");
    cityVertex2.setProperty("lat", "53.47497");
    cityVertex2.setProperty("long", "-2.25769");

    database.save(cityVertex2);
    database.createEdgeClass("owns");
    database.save(database.newEdge(countryVertex1, cityVertex1, "owns"));
    database.save(database.newEdge(countryVertex1, cityVertex2, "owns"));

    database.commit();
    String subquery = "select out('owns') as out from V where name = 'UK'";
    List<OResult> result = database.query(subquery).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((Collection) result.get(0).getProperty("out")).size(), 2);

    subquery = "select expand(out('owns')) from V where name = 'UK'";
    result = database.query(subquery).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (int i = 0; i < result.size(); i++) {
      //      System.out.println("uno: " + result.get(i));
      Assert.assertTrue(result.get(i).hasProperty("lat"));
    }

    String query =
        "select name, lat, long, distance(lat,long,51.5,0.08) as distance from (select"
            + " expand(out('owns')) from V where name = 'UK') order by distance";
    result = database.query(query).stream().collect(Collectors.toList());

    Assert.assertEquals(result.size(), 2);
    for (int i = 0; i < result.size(); i++) {
      //      System.out.println("dos: " + result.get(i));
      Assert.assertTrue(result.get(i).hasProperty("lat"));
      Assert.assertTrue(result.get(i).hasProperty("distance"));
    }
  }

  public void testDeleteOfVerticesWithDeleteCommandMustFail() {
    try {
      database.command("delete from GraphVehicle").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testDeleteOfEdgesWithDeleteCommandMustFail() {
    try {
      database.command("delete from E").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testDeleteOfVerticesAndEdgesWithDeleteCommandAndUnsafe() {
    OResultSet deletedVertices =
        database.command("delete from GraphVehicle return before limit 1 unsafe");
    Assert.assertTrue(deletedVertices.hasNext());

    OVertex v = deletedVertices.next().getVertex().get();

    Long confirmDeleted =
        database.command("delete from " + v.getIdentity() + " unsafe").next().getProperty("count");
    Assert.assertFalse(deletedVertices.hasNext());
    Assert.assertEquals(confirmDeleted.intValue(), 0);

    Iterable<OEdge> edges = v.getEdges(ODirection.BOTH);

    for (OEdge e : edges) {
      Long deletedEdges =
          database
              .command("delete from " + e.getIdentity() + " unsafe")
              .next()
              .getProperty("count");
      Assert.assertEquals(deletedEdges.intValue(), 1);
    }
  }

  public void testInsertOfEdgeWithInsertCommand() {
    try {
      database.command("insert into E set a = 33").close();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testInsertOfEdgeWithInsertCommandUnsafe() {

    OElement insertedEdge =
        database
            .command("insert into E set in = #9:0, out = #9:1, a = 33 unsafe")
            .next()
            .getElement()
            .get();

    Long confirmDeleted =
        database
            .command("delete from " + insertedEdge.getIdentity() + " unsafe")
            .next()
            .getProperty("count");
    Assert.assertEquals(confirmDeleted.intValue(), 1);
  }

  public void testEmbeddedDoc() {
    database.getMetadata().getSchema().createClass("NonVertex");

    OVertex vertex = database.newVertex("V");
    vertex.setProperty("name", "vertexWithEmbedded");
    database.save(vertex);

    ODocument doc = new ODocument();
    doc.field("foo", "bar");
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));

    vertex.setProperty("emb1", doc);

    ODocument doc2 = new ODocument("V");
    doc2.field("foo", "bar1");
    vertex.setProperty("emb2", doc2);

    ODocument doc3 = new ODocument("NonVertex");
    doc3.field("foo", "bar2");
    vertex.setProperty("emb3", doc3);

    Object res1 = vertex.getProperty("emb1");
    Assert.assertNotNull(res1);
    Assert.assertTrue(res1 instanceof ODocument);

    Object res2 = vertex.getProperty("emb2");
    Assert.assertNotNull(res2);
    Assert.assertFalse(res2 instanceof ODocument);

    Object res3 = vertex.getProperty("emb3");
    Assert.assertNotNull(res3);
    Assert.assertTrue(res3 instanceof ODocument);
    database.commit();
  }
}
