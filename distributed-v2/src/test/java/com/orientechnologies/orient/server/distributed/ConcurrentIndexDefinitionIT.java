package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.server.OServer;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ConcurrentIndexDefinitionIT {

  private OServer server0;
  private OServer server1;
  private OServer server2;
  private OrientDB remote;

  @Before
  public void before() throws Exception {
    server0 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-0.xml");
    server1 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-1.xml");
    server2 = OServer.startFromClasspathConfig("orientdb-simple-dserver-config-2.xml");
    remote = new OrientDB("remote:localhost", "root", "test", OrientDBConfig.defaultConfig());
    remote.create("test", ODatabaseType.PLOCAL);
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {

    ExecutorService executor = Executors.newFixedThreadPool(2);
    Future<?> future =
        executor.submit(
            () -> {
              ODatabaseSession session = remote.open("test", "admin", "admin");
              OClass clazz = session.createClass("Test");
              clazz.createProperty("test", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
            });
    Future<?> future1 =
        executor.submit(
            () -> {
              ODatabaseSession session = remote.open("test", "admin", "admin");
              OClass clazz = session.createClass("Test1");
              clazz.createProperty("test1", OType.STRING).createIndex(OClass.INDEX_TYPE.NOTUNIQUE);
            });
    future.get();
    future1.get();
    executor.shutdown();
    ODatabaseSession session = remote.open("test", "admin", "admin");
    assertTrue(session.getMetadata().getSchema().existsClass("Test"));
    assertFalse(session.getMetadata().getSchema().getClass("Test").getIndexes().isEmpty());

    assertTrue(session.getMetadata().getSchema().existsClass("Test1"));
    assertFalse(session.getMetadata().getSchema().getClass("Test1").getIndexes().isEmpty());

    OrientDB remote1 =
        new OrientDB("remote:localhost:2425", "root", "test", OrientDBConfig.defaultConfig());
    ODatabaseSession session1 = remote1.open("test", "admin", "admin");
    assertTrue(session1.getMetadata().getSchema().existsClass("Test"));
    assertFalse(session1.getMetadata().getSchema().getClass("Test").getIndexes().isEmpty());

    assertTrue(session1.getMetadata().getSchema().existsClass("Test1"));
    assertFalse(session1.getMetadata().getSchema().getClass("Test1").getIndexes().isEmpty());
    session1.close();
    remote1.close();

    OrientDB remote2 =
        new OrientDB("remote:localhost:2426", "root", "test", OrientDBConfig.defaultConfig());
    // Make sure the created database is propagated
    ODatabaseSession session2 = remote2.open("test", "admin", "admin");
    assertTrue(session2.getMetadata().getSchema().existsClass("Test"));
    assertFalse(session2.getMetadata().getSchema().getClass("Test").getIndexes().isEmpty());

    assertTrue(session2.getMetadata().getSchema().existsClass("Test1"));
    assertFalse(session2.getMetadata().getSchema().getClass("Test1").getIndexes().isEmpty());
    session2.close();
    remote2.close();
  }

  @After
  public void after() {
    remote.drop("test");
    remote.close();

    server0.shutdown();
    server1.shutdown();
    server2.shutdown();
    ODatabaseDocumentTx.closeAll();
  }
}
