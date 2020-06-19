package com.orientechnologies.tinkerpop;

import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.id.ORecordId;
import java.util.*;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.orientdb.OrientEdge;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientVertex;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResult;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 26/01/17. */
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

    OGremlinResultSet gremlin =
        noTx.execute("gremlin", "g.V().hasLabel('Person').has('name','Luke')", null);

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

    v1.addEdge("HasFriend", v2, "since", new Date());

    OGremlinResultSet gremlin = noTx.execute("gremlin", "g.E().hasLabel('HasFriend')", null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();
    OrientEdge vertex = result.getEdge().get();
    Assert.assertNotNull(vertex.value("since"));
  }

  @Test
  public void testExecuteGremlinAggregateTest() {

    OrientGraph noTx = factory.getNoTx();

    Vertex v1 = noTx.addVertex(T.label, "Person", "name", "John");
    Vertex v2 = noTx.addVertex(T.label, "Person", "name", "Luke");

    v1.addEdge("HasFriend", v2, "since", new Date());

    OGremlinResultSet gremlin =
        noTx.execute(
            "gremlin",
            "g.V().hasLabel('Person').has('name','Luke').as('luke').branch{it.get().label()}.option('Person',__.in('HasFriend').aggregate('friends')).select('luke','friends')",
            null);

    List<OGremlinResult> collected = gremlin.stream().collect(Collectors.toList());
    Assert.assertEquals(1, collected.size());

    OGremlinResult result = collected.iterator().next();

    Object luke = result.getProperty("luke");

    Assert.assertTrue(luke instanceof ORecordId);

    List friends = result.getProperty("friends");

    Assert.assertEquals(1, friends.size());

    Object john = friends.iterator().next();

    Assert.assertTrue(john instanceof ORecordId);
  }

  @Test(expected = OCommandExecutionException.class)
  public void testExecuteGremlinWithError() {

    OrientGraph noTx = factory.getNoTx();

    try (OGremlinResultSet gremlin =
        noTx.execute(
            "gremlin",
            "g.V().hasLabel('Person').has('name','Luke').as('luke').branch{it.get().label()}.option('Person',__.in('HasFriend').aggregate('friends')).select('luke','friends').",
            null)) {}
  }

  @Test(expected = OCommandExecutionException.class)
  public void testExecuteGremlinWithError2() {

    OrientGraph noTx = factory.getNoTx();

    Map<String, String> params = new HashMap<>();

    try (OGremlinResultSet gremlin = noTx.execute("gremlin", "g.V().select($test)", params)) {}
  }
}
