package com.orientechnologies.tinkerpop;

import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResult;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 26/01/17.
 */
public class OrientGraphExecuteQueryRemoteGraphFactoryTest extends AbstractRemoteGraphFactoryTest {


  @Test
  public void testExecuteGremlinSimpleQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.V()", null);

    Assert.assertEquals(2, gremlin.stream().count());
  }


  @Test
  public void testExecuteGremlinCountQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.V().count()", null);

    Iterator<OGremlinResult> iterator = gremlin.iterator();
    Assert.assertEquals(true, iterator.hasNext());
    OGremlinResult result = iterator.next();
    Long count = result.getProperty("value");
    Assert.assertEquals(new Long(2), count);

  }


  @Test
  public void testExecuteGremlinVertexQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    noTx.addVertex(T.label, "Person", "name", "John");
    noTx.addVertex(T.label, "Person", "name", "Luke");

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.V().hasLabel('Person').has('name','Luke')", null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();
    OrientVertex vertex = result.getVertex().get();
    Assert.assertEquals("Luke", vertex.value("name"));

  }

  @Test
  public void testExecuteGremlinEdgeQueryTest() {

    OrientGraph noTx = factory.getNoTx();

    Vertex v1 = noTx.addVertex(T.label, "Person", "name", "John");
    Vertex v2 = noTx.addVertex(T.label, "Person", "name", "Luke");

    v1.addEdge("HasFriend",v2, "since", new Date());


    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.E().hasLabel('HasFriend')", null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();
    OrientEdge vertex = result.getEdge().get();
    Assert.assertNotNull(vertex.value("since"));

  }
}
