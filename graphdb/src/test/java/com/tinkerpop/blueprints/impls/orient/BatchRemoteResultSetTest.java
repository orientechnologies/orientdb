package com.tinkerpop.blueprints.impls.orient;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.script.OCommandScript;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.test.server.network.http.BaseHttpTest;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Iterator;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 24/05/16. */
public class BatchRemoteResultSetTest {
  private String serverHome;
  private String oldOrientDBHome;

  private OServer server;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, MBeanRegistrationException, InvocationTargetException,
          NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {

    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + File.separator + BaseHttpTest.class.getSimpleName();

    File file = new File(serverHome);
    deleteDirectory(file);
    file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    server = new OServer(false);
    server.startup(
        OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));
    server.activate();
    OServerAdmin admin = new OServerAdmin("remote:localhost:3064");
    admin.connect("root", "root");
    admin.createDatabase(OrientGraphRemoteTest.class.getSimpleName(), "graph", "memory");
    admin.close();
  }

  @Test
  public void runBatchQuery() {

    String batchQuery =
        "begin; LET t0 = CREATE VERTEX V set mame=\"a\" ;\n LET t1 = CREATE VERTEX V set name=\"b\" ;\n"
            + "LET t2 = CREATE EDGE E FROM $t0 TO $t1 ;\n commit retry 100\n"
            + "return [$t0,$t1,$t2]";

    OrientGraph graph =
        new OrientGraph(
            "remote:localhost:3064/" + OrientGraphRemoteTest.class.getSimpleName(), "root", "root");

    Iterable<OIdentifiable> res =
        graph.getRawGraph().command(new OCommandScript("sql", batchQuery)).execute();
    Iterator iter = res.iterator();
    assertTrue(iter.next() instanceof OIdentifiable);
    assertTrue(iter.next() instanceof OIdentifiable);
    Object edges = iter.next();
    assertTrue(edges instanceof Collection);
    assertTrue(((Collection) edges).iterator().next() instanceof OIdentifiable);
  }

  @After
  public void after() throws Exception {
    server.shutdown();

    if (oldOrientDBHome != null) System.setProperty("ORIENTDB_HOME", oldOrientDBHome);
    else System.clearProperty("ORIENTDB_HOME");

    Thread.sleep(1000);
    ODatabaseDocumentTx.closeAll();

    File file = new File(serverHome);
    deleteDirectory(file);

    Orient.instance().startup();
  }

  @AfterClass
  public static void afterClass() {
    Orient.instance().shutdown();
    Orient.instance().startup();
  }

  protected static void deleteDirectory(File directory) {
    if (directory.exists()) {
      File[] files = directory.listFiles();
      int len = files.length;

      for (int i = 0; i < len; ++i) {
        File file = files[i];
        if (file.isDirectory()) {
          deleteDirectory(file);
        } else {
          file.delete();
        }
      }

      directory.delete();
    }

    if (directory.exists()) {
      throw new RuntimeException("unable to delete directory " + directory.getAbsolutePath());
    }
  }
}
