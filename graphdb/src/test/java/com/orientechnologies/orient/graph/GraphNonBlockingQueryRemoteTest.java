package com.orientechnologies.orient.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import com.orientechnologies.orient.server.OServer;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphRemoteTest;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import javax.management.InstanceAlreadyExistsException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import javax.management.NotCompliantMBeanException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 01/07/16. */
public class GraphNonBlockingQueryRemoteTest {

  private OServer server;
  private String serverHome;
  private String oldOrientDBHome;

  @Before
  public void before()
      throws ClassNotFoundException, MalformedObjectNameException, InstanceAlreadyExistsException,
          NotCompliantMBeanException, MBeanRegistrationException, InvocationTargetException,
          NoSuchMethodException, InstantiationException, IOException, IllegalAccessException {

    final String buildDirectory = System.getProperty("buildDirectory", ".");
    serverHome = buildDirectory + "/" + GraphNonBlockingQueryRemoteTest.class.getSimpleName();

    deleteDirectory(new File(serverHome));

    final File file = new File(serverHome);
    Assert.assertTrue(file.mkdir());

    oldOrientDBHome = System.getProperty("ORIENTDB_HOME");
    System.setProperty("ORIENTDB_HOME", serverHome);

    server = new OServer(false);
    server.startup(
        OrientGraphRemoteTest.class.getResourceAsStream("/embedded-server-config-single-run.xml"));

    server.activate();
    try (OrientDB orientDB =
        new OrientDB("remote:localhost:3064", "root", "root", OrientDBConfig.defaultConfig())) {
      orientDB.execute(
          "create database ? memory users(admin identified by 'admin' role admin)",
          GraphNonBlockingQueryRemoteTest.class.getSimpleName());
    }
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
  public void testNonBlockingClose() throws ExecutionException, InterruptedException {
    OrientGraph database =
        new OrientGraph(
            "remote:localhost:3064/" + GraphNonBlockingQueryRemoteTest.class.getSimpleName());
    database.createVertexType("Prod").createProperty("something", OType.STRING);
    for (int i = 0; i < 21; i++) {
      OrientVertex vertex = database.addVertex("class:Prod");
      vertex.setProperty("something", "value");
      vertex.save();
    }
    database.commit();
    final CountDownLatch ended = new CountDownLatch(21);
    try {
      OSQLNonBlockingQuery<Object> test =
          new OSQLNonBlockingQuery<Object>(
              "select * from Prod ",
              new OCommandResultListener() {
                int resultCount = 0;

                @Override
                public boolean result(Object iRecord) {
                  resultCount++;

                  ODocument odoc = ((ODocument) iRecord);
                  for (String name : odoc.fieldNames()) { // <----------- PROBLEM
                    assertEquals("something", name);
                  }
                  ended.countDown();
                  return resultCount > 20 ? false : true;
                }

                @Override
                public void end() {
                  ended.countDown();
                }

                @Override
                public Object getResult() {
                  return resultCount;
                }
              });

      database.command(test).execute();

      assertTrue(ended.await(10, TimeUnit.SECONDS));

    } finally {
      database.shutdown();
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
