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

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Test
public class GraphDatabaseTest extends DocumentDBBaseTest {
  private OrientGraph database;

  @Parameters(value = "url")
  public GraphDatabaseTest(@Optional String url) {
    super(url);
  }

  @BeforeMethod
  public void init() {
    database = new OrientGraph(url);
    database.setUseLightweightEdges(false);
  }

  @AfterMethod
  public void deinit() {
    database.shutdown();
  }

  @Test
  public void populate() {
    OClass vehicleClass = database.createVertexType("GraphVehicle");
    database.createVertexType("GraphCar", vehicleClass);
    database.createVertexType("GraphMotocycle", "GraphVehicle");

    ODocument carNode = database.addVertex("class:GraphCar", "brand", "Hyundai", "model", "Coupe", "year", 2003).getRecord();
    ODocument motoNode = database.addVertex("class:GraphMotocycle", "brand", "Yamaha", "model", "X-City 250", "year", 2009)
        .getRecord();

    database.commit();
    database.addEdge(null, database.getVertex(carNode), database.getVertex(motoNode), "E").save();

    List<ODocument> result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>("select from GraphVehicle"));
    Assert.assertEquals(result.size(), 2);
    for (ODocument v : result) {
      Assert.assertTrue(v.getSchemaClass().isSubClassOf(vehicleClass));
    }

    database.shutdown();

    database = new OrientGraph(url);
    database.setUseLightweightEdges(false);

    database.getRawGraph().getMetadata().getSchema().reload();

    result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>("select from GraphVehicle"));
    Assert.assertEquals(result.size(), 2);

    Edge edge1 = null;
    Edge edge2 = null;

    for (ODocument v : result) {
      Assert.assertTrue(v.getSchemaClass().isSubClassOf("GraphVehicle"));

      if (v.getClassName().equals("GraphCar")) {
        Assert.assertEquals(database.getVertex(v).countEdges(Direction.OUT), 1);
        edge1 = database.getVertex(v).getEdges(Direction.OUT).iterator().next();
      } else {
        Assert.assertEquals(database.getVertex(v).countEdges(Direction.IN), 1);
        edge2 = database.getVertex(v).getEdges(Direction.IN).iterator().next();
      }
    }

    Assert.assertEquals(edge1, edge2);
  }

  @Test(dependsOnMethods = "populate")
  public void testSQLAgainstGraph() {
    Vertex tom = database.addVertex(null, "name", "Tom");
    Vertex ferrari = database.addVertex("class:GraphCar", "brand", "Ferrari");
    Vertex maserati = database.addVertex("class:GraphCar", "brand", "Maserati");
    Vertex porsche = database.addVertex("class:GraphCar", "brand", "Porsche");
    database.addEdge(null, tom, ferrari, "drives");
    database.addEdge(null, tom, maserati, "drives");
    database.addEdge(null, tom, porsche, "owns");
    database.commit();

    Assert.assertNotNull(database.getVertex(tom).getEdges(Direction.OUT, "drives"));
    Assert.assertEquals(database.getVertex(tom).countEdges(Direction.OUT, "drives"), 2);

    List<ODocument> result = database.getRawGraph().query(
        new OSQLSynchQuery<ODocument>("select out_[in.@class = 'GraphCar'].in_ from V where name = 'Tom'"));
    Assert.assertEquals(result.size(), 1);

    result = database.getRawGraph().query(
        new OSQLSynchQuery<ODocument>("select out_[label='drives'][in.brand = 'Ferrari'].in_ from V where name = 'Tom'"));
    Assert.assertEquals(result.size(), 1);

    result = database.getRawGraph().query(
        new OSQLSynchQuery<ODocument>("select out_[in.brand = 'Ferrari'].in_ from V where name = 'Tom'"));
    Assert.assertEquals(result.size(), 1);
  }

  public void testNotDuplicatedIndexTxChanges() throws IOException {
    database.setAutoStartTx(false);
    database.commit();
    OClass oc = database.getVertexType("vertexA");
    if (oc == null)
      oc = database.createVertexType("vertexA");
    if (!oc.existsProperty("name"))
      oc.createProperty("name", OType.STRING);

    if (oc.getClassIndex("vertexA_name_idx") == null)
      oc.createIndex("vertexA_name_idx", OClass.INDEX_TYPE.UNIQUE, "name");

    database.setAutoStartTx(true);
    // FIRST: create a couple of records
    Vertex vertexA = database.addVertex("class:vertexA", "name", "myKey");
    Vertex vertexB = database.addVertex("class:vertexA", "name", "anotherKey");
    database.commit();

    database.removeVertex(vertexB);
    database.removeVertex(vertexA);
    database.addVertex("class:vertexA", "name", "myKey");
    database.commit();
  }

  public void testNewVertexAndEdgesWithFieldsInOneShoot() throws IOException {
    OrientVertex vertexA = database.addVertex(null, "field1", "value1", "field2", "value2");

    Map<String, Object> map = new HashMap<String, Object>();
    map.put("field1", "value1");
    map.put("field2", "value2");
    OrientVertex vertexB = database.addVertex(null, map);

    OrientEdge edgeC = database.addEdge(null, vertexA, vertexB, "E");
    edgeC.setProperty("edgeF1", "edgeV2");

    database.commit();

    Assert.assertEquals(vertexA.getProperty("field1"), "value1");
    Assert.assertEquals(vertexA.getProperty("field2"), "value2");

    Assert.assertEquals(vertexB.getProperty("field1"), "value1");
    Assert.assertEquals(vertexB.getProperty("field2"), "value2");

    Assert.assertEquals(edgeC.getProperty("edgeF1"), "edgeV2");
  }

  @Test
  public void sqlNestedQueries() {
    Vertex vertex1 = database.addVertex(null, "driver", "John");
    Vertex vertex2 = database.addVertex(null, "car", "ford");

    Vertex targetVertex = database.addVertex(null, "car", "audi");

    Edge edge = database.addEdge(null, vertex1, vertex2, "E");
    edge.setProperty("color", "red");
    edge.setProperty("action", "owns");

    edge = database.addEdge(null, vertex1, targetVertex, "E");
    edge.setProperty("color", "red");
    edge.setProperty("action", "wants");

    database.commit();

    String query1 = "select driver from V where out().car contains 'ford'";
    List<ODocument> result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(query1));
    Assert.assertEquals(result.size(), 1);

    String query2 = "select driver from V where outE()[color='red'].inV().car contains 'ford'";
    result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(query2));
    Assert.assertEquals(result.size(), 1);

    //TODO these tests are broken, they should test "contains" instead of "="
    String query3 = "select driver from V where outE()[action='owns'].inV().car = 'ford'";
    result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(query3));
    Assert.assertEquals(result.size(), 1);

    String query4 = "select driver from V where outE()[color='red'][action='owns'].inV().car = 'ford'";
    result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(query4));
    Assert.assertEquals(result.size(), 1);
  }

  @SuppressWarnings("unchecked")
  public void nestedQuery() {
    Vertex countryVertex1 = database.addVertex(null, "name", "UK", "area", "Europe", "code", "2");
    Vertex cityVertex1 = database.addVertex(null, "name", "leicester", "lat", "52.64640", "long", "-1.13159");
    Vertex cityVertex2 = database.addVertex(null, "name", "manchester", "lat", "53.47497", "long", "-2.25769");

    database.addEdge(null, countryVertex1, cityVertex1, "owns");
    database.addEdge(null, countryVertex1, cityVertex2, "owns");

    database.commit();
    String subquery = "select out('owns') from V where name = 'UK'";
    List<OIdentifiable> result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(subquery));

    Assert.assertEquals(result.size(), 1);
    Assert.assertEquals(((Collection) ((ODocument) result.get(0)).field("out")).size(), 2);

    subquery = "select expand(out('owns')) from V where name = 'UK'";
    result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(subquery));

    Assert.assertEquals(result.size(), 2);
    for (int i = 0; i < result.size(); i++) {
      //      System.out.println("uno: " + result.get(i));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("lat"));
    }

    String query = "select name, lat, long, distance(lat,long,51.5,0.08) as distance from (select expand(out('owns')) from V where name = 'UK') order by distance";
    result = database.getRawGraph().query(new OSQLSynchQuery<ODocument>(query));

    Assert.assertEquals(result.size(), 2);
    for (int i = 0; i < result.size(); i++) {
      //      System.out.println("dos: " + result.get(i));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("lat"));
      Assert.assertTrue(((ODocument) result.get(i).getRecord()).containsField("distance"));
    }
  }

  public void testDeleteOfVerticesWithDeleteCommandMustFail() {
    try {
      database.command(new OCommandSQL("delete from GraphVehicle")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testDeleteOfEdgesWithDeleteCommandMustFail() {
    try {
      database.command(new OCommandSQL("delete from E")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }
  }

  public void testDeleteOfVerticesAndEdgesWithDeleteCommandAndUnsafe() {
    Iterable<OIdentifiable> deletedVertices = database.command(
        new OCommandSQL("delete from GraphVehicle return before limit 1 unsafe")).execute();
    Assert.assertTrue(deletedVertices.iterator().hasNext());

    OrientVertex v = (OrientVertex) deletedVertices.iterator().next();

    Integer confirmDeleted = database.command(new OCommandSQL("delete from " + v.getIdentity() + " unsafe")).execute();
    Assert.assertFalse(deletedVertices.iterator().hasNext());
    Assert.assertEquals(confirmDeleted.intValue(), 0);

    Iterable<Edge> edges = v.getEdges(Direction.BOTH);

    for (Edge e : edges) {
      Integer deletedEdges = database.command(new OCommandSQL("delete from " + ((OrientEdge) e).getIdentity() + " unsafe"))
          .execute();
      Assert.assertEquals(deletedEdges.intValue(), 1);
    }

  }

  public void testInsertOfEdgeWithInsertCommand() {
    try {
      database.command(new OCommandSQL("insert into E set a = 33")).execute();
      Assert.assertTrue(false);
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

  }

  public void testInsertOfEdgeWithInsertCommandUnsafe() {

    OrientEdge insertedEdge = database.command(new OCommandSQL("insert into E set in = #9:0, out = #9:1, a = 33 unsafe")).execute();
    Assert.assertNotNull(insertedEdge);

    Integer confirmDeleted = database.command(new OCommandSQL("delete from " + insertedEdge.getIdentity() + " unsafe")).execute();
    Assert.assertEquals(confirmDeleted.intValue(), 1);
  }

  public void checkSetPropertyCustomInTransaction() {
    OrientVertexType typedef = database.createVertexType("SetPropertyCustomInTransaction");
    OrientVertexType.OrientVertexProperty prop = typedef.createProperty("foo", OType.STRING);
    prop.setCustom("someparam", "param value");
    Assert.assertEquals(prop.getCustom("someparam"), "param value");
    prop.setMax("10");
    Assert.assertEquals(prop.getMax(), "10");
    prop.setMin("10");
    Assert.assertEquals(prop.getMin(), "10");
    prop.setDefaultValue("FooBarBaz1");
    Assert.assertEquals(prop.getDefaultValue(), "FooBarBaz1");
  }

}
