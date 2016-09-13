package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 06/07/16.
 */
public class ORemoteDBFactoryTest {

  private static final String SERVER_DIRECTORY = "./target/dbfactory";
  private OServer server;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer();
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(
        getClass().getClassLoader().getResourceAsStream("com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();
  }

  @Test
  public void createAndUseRemoteDatabase() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("remote:localhost", null);
    if (!factory.exists("test", "root", "root"))
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    factory.close();
  }

  //@Test(expected = OStorageExistsException.class)
  //TODO: Uniform database exist exceptions
  @Test(expected = OStorageException.class)
  public void doubleCreateRemoteDatabase() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("remote:localhost", null);
    try {
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
    } finally {
      factory.close();
    }
  }

  @Test
  public void createDropRemoteDatabase() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("remote:localhost", null);
    try {
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
      assertTrue(factory.exists("test", "root", "root"));
      factory.drop("test", "root", "root");
      assertFalse(factory.exists("test", "root", "root"));
    } finally {
      factory.close();
    }
  }


  @Test
  public void testPool() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    if (!factory.exists("test", "root", "root"))
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);

    OPool<ODatabaseDocument> pool = factory.openPool("test", "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument());
    db.close();
    pool.close();
    factory.close();
  }

  @Test
  public void testListDatabases() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);
    assertEquals(factory.listDatabases("root", "root").size(), 0);
    factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
    Set<String> databases = factory.listDatabases("root", "root");
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
  }

  @Test
  public void testCopyOpenedDatabase() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    // OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
    ODatabaseDocument db1;
    try(ODatabaseDocumentInternal db = (ODatabaseDocumentInternal)factory.open("test", "admin", "admin")){
      db1 =db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
  }
  
  @After
  public void after() {
    server.shutdown();
    Orient.instance().startup();
  }

}
