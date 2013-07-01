package com.orientechnologies.orient.graph.blueprints;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.graph.OGraphDatabase;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.graph.gremlin.OCommandGremlin;
import com.orientechnologies.orient.graph.gremlin.OGremlinHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class RemoteGremlinTest {
  /**
   * 
   */
  private static final String URL = "remote:localhost/tinkerpop";
  private final OServer       server;
  private OGraphDatabase      graphDatabase;

  public RemoteGremlinTest() throws Exception {
    if (System.getProperty("ORIENTDB_HOME") == null) {
      System.setProperty("ORIENTDB_HOME", "target");
    }
    OGremlinHelper.global().create();
    server = OServerMain.create();
  }

  @BeforeClass
  public void setUp() throws Exception {
    server.startup(new File(getClass().getResource("db-config.xml").getFile()));
    server.activate();
  }

  @AfterClass
  public void tearDown() throws Exception {
    if (server != null)
      server.shutdown();
  }

  @BeforeMethod
  public void beforeMethod() {
    for (OStorage stg : Orient.instance().getStorages()) {
      System.out.println("Closing storage: " + stg);
      stg.close(true);
    }

    graphDatabase = new OGraphDatabase(URL);
    graphDatabase.open("admin", "admin");
  }

  @AfterMethod
  public void afterMethod() {
    graphDatabase.close();
  }

  @Test
  public void function() throws IOException {
    ODocument vertex1 = (ODocument) graphDatabase.createVertex().field("label", "car").save();
    ODocument vertex2 = (ODocument) graphDatabase.createVertex().field("label", "pilot").save();
    ODocument edge = (ODocument) graphDatabase.createEdge(vertex1, vertex2).field("label", "drives").save();

    List<?> result = graphDatabase.query(new OSQLSynchQuery<Object>(
        "select gremlin('current.out.in') as value from V where out.size() > 0 limit 3"));
    System.out.println("Query result: " + result);

    result = graphDatabase.query(new OSQLSynchQuery<Object>("select gremlin('current.out') as value from V"));
    System.out.println("Query result: " + result);

    int clusterId = graphDatabase.getVertexBaseClass().getDefaultClusterId();

    result = graphDatabase.query(new OSQLSynchQuery<Object>("select gremlin('current.out.in') as value from " + clusterId + ":1"));
    System.out.println("Query result: " + result);

    result = graphDatabase.query(new OSQLSynchQuery<Object>("select gremlin('current.out(\"drives\").count()') as value from V"));
    System.out.println("Query result: " + result);
  }

  @Test
  public void command() throws IOException {
    List<OIdentifiable> result = graphDatabase.command(new OCommandGremlin("g.V[0..10]")).execute();
    if (result != null) {
      for (OIdentifiable doc : result) {
        System.out.println(doc.getRecord().toJSON());
      }
    }

    Map<String, Object> params = new HashMap<String, Object>();
    params.put("par1", 100);

    result = graphDatabase.command(new OCommandSQL("select gremlin('current.out.filter{ it.performances > par1 }') from V"))
        .execute(params);
    System.out.println("Command result: " + result);
  }

  @Test
  public void testGremlinAgainstBlueprints() {
    OGremlinHelper.global().create();

    OrientGraph graph = new OrientGraph(URL);

    final int NUM_ITERS = 1000;

    long start = System.currentTimeMillis();
    try {
      for (int i = NUM_ITERS; i > 0; i--) {
        List<Vertex> r = graph.getRawGraph().command(new OCommandGremlin("g.V[1].out.out.in")).execute();
        System.out.println(r.size());
      }

      System.out.println("Total: " + (System.currentTimeMillis() - start) + " ms AVG: "
          + ((System.currentTimeMillis() - start) / (float) NUM_ITERS));

    } catch (Exception x) {
      x.printStackTrace();
      System.out.println(graph.getRawGraph().isClosed());
    }
  }

  @Test
  public void testRemoteDB() {
    OrientGraphNoTx graph = new OrientGraphNoTx("remote:localhost/temp", "admin", "admin");

    OrientVertex v1 = graph.addVertex("class:V");
    OrientVertex v2 = graph.addVertex("class:V");
    OrientVertex v3 = graph.addVertex("class:V");

    v1.addEdge("rel", v2);
    v2.addEdge("rel", v3);

    graph.commit();

    graph = new OrientGraphNoTx("remote:localhost/temp");

    Assert.assertEquals(graph.countVertices(), 3);
    Assert.assertEquals(graph.countEdges(), 2);

    graph.shutdown();
  }

}
