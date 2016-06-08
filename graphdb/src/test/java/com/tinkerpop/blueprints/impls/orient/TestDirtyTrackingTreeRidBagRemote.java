package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by tglman on 04/05/16.
 */
public class TestDirtyTrackingTreeRidBagRemote {

  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException, NotCompliantMBeanException,
      MBeanRegistrationException, InvocationTargetException, NoSuchMethodException, InstantiationException, IOException,
      IllegalAccessException {
    server = new OServer(false);
    server.startup(OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));
    server.activate();
    OServerAdmin admin = new OServerAdmin("remote:localhost:3064");
    admin.connect("root", "root");
    admin.createDatabase(TestDirtyTrackingTreeRidBagRemote.class.getSimpleName(), "graph", "memory");
    admin.close();
  }

  @After
  public void after() {
    server.shutdown();
    Orient.instance().startup();
  }

  @Test
  public void test() {
    final int max = OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    OrientGraph graph = new OrientGraph("remote:localhost:3064/" + TestDirtyTrackingTreeRidBagRemote.class.getSimpleName(), "root",
        "root");
    graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
    graph.createEdgeType("Edge");
    OIdentifiable oneVertex = null;
    Map<Object, Vertex> vertices = new HashMap<Object, Vertex>();
    for (int i = 0; i < max; i++) {
      Vertex v = graph.addVertex("class:V");
      v.setProperty("key", "foo" + i);
      graph.commit();
      vertices.put(v.getProperty("key"), v);
      if (i == max / 2 + 1)
        oneVertex = ((OrientVertex) v).getIdentity();
    }
    graph.commit();
    // Add the edges
    for (int i = 0; i < max; i++) {
      String codeUCD1 = "foo" + i;
      // Take the first vertex
      Vertex med1 = (Vertex) vertices.get(codeUCD1);
      // For the 2nd term
      for (int j = 0; j < max; j++) {
        String key = "foo" + j;
        // Take the second vertex
        Vertex med2 = (Vertex) vertices.get(key);
        //        ((OrientVertex)med2).getRecord().reload();
        OrientEdge eInteraction = graph.addEdge(null, med1, med2, "Edge");
        assertNotNull(graph.getRawGraph().getTransaction().getRecordEntry(((OrientVertex) med2).getIdentity()));
      }
      // COMMIT
      graph.commit();
    }
    graph.getRawGraph().getLocalCache().clear();

    OrientVertex vertex = graph.getVertex(oneVertex);
    assertEquals(new GremlinPipeline<Vertex, Long>().start(vertex).in("Edge").count(), max);
  }

}
