package com.orientechnologies.orient.graph;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphRemoteTest;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.junit.*;

import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Created by tglman on 01/07/16. */
public class GraphLinkVertexIssueRemoteTest {

  private OServer server;
  private String serverHome;
  private String oldOrientDBHome;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, MBeanRegistrationException, InvocationTargetException,
          NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {

    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + "/" + GraphLinkVertexIssueRemoteTest.class.getSimpleName();

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
    admin.createDatabase(GraphLinkVertexIssueRemoteTest.class.getSimpleName(), "graph", "memory");
    admin.close();
  }

  @After
  public void after() {
    server.shutdown();

    if (oldOrientDBHome != null) System.setProperty("ORIENTDB_HOME", oldOrientDBHome);
    else System.clearProperty("ORIENTDB_HOME");
  }

  @AfterClass
  public static void afterClass() {
    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  @Test
  public void testLinkIssue() {
    final OrientGraphFactory factory =
        new OrientGraphFactory(
            "remote:localhost:3064/" + GraphLinkVertexIssueRemoteTest.class.getSimpleName());
    final OrientGraph graph = factory.getTx();

    try {
      final OrientVertex app1 = graph.addVertex("class:app1");
      app1.setProperty("ID", UUID.randomUUID().toString());
      app1.setProperty("LID", UUID.randomUUID().toString());
      app1.setProperty("current", true);
      app1.setProperty("insertedOn", System.currentTimeMillis());
      app1.setProperty("version", 1);
      final OrientVertex app2 = graph.addVertex("class:app2");
      app1.addEdge("UNDER", app2);

      final OrientVertex app3 = graph.addVertex("class:app3");

      final OrientVertex new1 = graph.addVertex("class:new1");
      final OrientVertex new2 = graph.addVertex("class:new2");
      graph.commit();

      // "create a new vertex and create an edge which has APP3 vertex as target"
      new1.addEdge("OK", app3);

      // create a new vertex and create an edge which has APP1 or APP2 vertex as target (which does
      // have any other in or out edges) transaction fails indicating this exception:
      // java.lang.AssertionError: Cx ollection timeline required for link types serialization
      new2.addEdge("NOT_OK?", app1);
      new2.setProperty("insertedOn", System.currentTimeMillis());
      new2.setProperty("isActive", true);
      new2.setProperty("isCurrent", true);
      new2.setProperty("versioning", 1);
      new2.setProperty("since", System.currentTimeMillis());
    } finally {
      graph.commit();
      graph.shutdown();
    }
  }

  private static void deleteDirectory(File f) throws IOException {
    if (f.isDirectory()) {
      final File[] files = f.listFiles();
      if (files != null) {
        for (File c : files) deleteDirectory(c);
      }
    }
    if (f.exists() && !f.delete()) throw new FileNotFoundException("Failed to delete file: " + f);
  }
}
