package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Iterator;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

public class SelectProjectionVertexRemoteTest {

  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, MBeanRegistrationException, InvocationTargetException,
          NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
    server = new OServer(false);
    server.startup(
        OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));
    server.activate();
    OServerAdmin admin = new OServerAdmin("remote:localhost:3064");
    admin.connect("root", "root");
    admin.createDatabase(SelectProjectionVertexRemoteTest.class.getSimpleName(), "graph", "memory");
    admin.close();
  }

  @After
  public void after() {
    server.shutdown();
  }

  @AfterClass
  public static void afterClass() {
    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  @Test
  public void test() {
    OrientGraph graph =
        new OrientGraph(
            "remote:localhost:3064/" + SelectProjectionVertexRemoteTest.class.getSimpleName());
    try {
      graph.createVertexType("VertA");
      graph.createVertexType("VertB");
      graph.createEdgeType("AtoB");
      OrientVertex root = graph.addVertex("class:VertA");
      graph.commit();

      for (int i = 0; i < 2; i++) {
        OrientVertex v = graph.addVertex("class:VertB");
        root.addEdge("AtoB", v);
      }
      graph.commit();

      String query =
          "SELECT $res as val LET $res = (SELECT @rid AS refId, out('AtoB') AS vertices FROM VertA) FETCHPLAN val:2";

      Iterable<OrientVertex> results = graph.command(new OCommandSQL(query)).execute();
      final Iterator<OrientVertex> iterator = results.iterator();
      assertTrue(iterator.hasNext());
      OrientVertex result = iterator.next();

      Iterable<OrientVertex> vertices = result.getProperty("val");
      for (OrientVertex vertex : vertices) {
        assertEquals(
            ((OIdentifiable) vertex.getProperty("refId")).getIdentity(), root.getIdentity());
      }
    } finally {
      graph.shutdown();
    }
  }
}
