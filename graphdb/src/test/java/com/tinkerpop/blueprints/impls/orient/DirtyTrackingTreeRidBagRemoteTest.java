package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.gremlin.java.GremlinPipeline;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.*;

/** Created by tglman on 04/05/16. */
public class DirtyTrackingTreeRidBagRemoteTest {
  private OServer server;
  private String serverHome;
  private String oldOrientDBHome;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, MBeanRegistrationException, InvocationTargetException,
          NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {
    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + "/" + DirtyTrackingTreeRidBagRemoteTest.class.getSimpleName();
    deleteDirectory(new File(serverHome));

    final File file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    server = new OServer(false);
    server.startup(
        OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));
    server.activate();
    OServerAdmin admin = new OServerAdmin("remote:localhost:3064");
    admin.connect("root", "root");
    admin.createDatabase(
        DirtyTrackingTreeRidBagRemoteTest.class.getSimpleName(), "graph", "memory");
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
  @Ignore
  public void test() {
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getDefValue());
    final int max =
        OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger() * 2;
    final OrientGraph graph =
        new OrientGraph(
            "remote:localhost:3064/" + DirtyTrackingTreeRidBagRemoteTest.class.getSimpleName(),
            "root",
            "root");

    try {
      graph.getRawGraph().declareIntent(new OIntentMassiveInsert());
      graph.createEdgeType("Edge");
      OIdentifiable oneVertex = null;
      final Map<Object, Vertex> vertices = new HashMap<Object, Vertex>();
      for (int i = 0; i < max; i++) {
        final Vertex v = graph.addVertex("class:V");
        v.setProperty("key", "foo" + i);
        graph.commit();
        vertices.put(v.getProperty("key"), v);
        if (i == max / 2 + 1) oneVertex = ((OrientVertex) v).getIdentity();
      }
      graph.commit();
      // Add the edges
      for (int i = 0; i < max; i++) {
        final String codeUCD1 = "foo" + i;
        // Take the first vertex
        final Vertex med1 = vertices.get(codeUCD1);
        // For the 2nd term
        for (int j = 0; j < max; j++) {
          final String key = "foo" + j;
          // Take the second vertex
          final Vertex med2 = vertices.get(key);
          // ((OrientVertex)med2).getRecord().reload();
          graph.addEdge(null, med1, med2, "Edge");
          assertNotNull(
              graph
                  .getRawGraph()
                  .getTransaction()
                  .getRecordEntry(((OrientVertex) med2).getIdentity()));
        }
        // COMMIT
        graph.commit();
      }
      graph.getRawGraph().getLocalCache().clear();

      final OrientVertex vertex = graph.getVertex(oneVertex);
      assertEquals(new GremlinPipeline<Vertex, Long>().start(vertex).in("Edge").count(), max);
    } finally {
      graph.shutdown();
    }
  }

  private static void deleteDirectory(final File f) throws IOException {
    if (f.isDirectory()) {
      final File[] files = f.listFiles();
      if (files != null) {
        for (final File c : files) deleteDirectory(c);
      }
    }
    if (f.exists() && !f.delete()) throw new FileNotFoundException("Failed to delete file: " + f);
  }
}
