package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Created by tglman on 06/07/16.
 */
public class OrientDBRemoteTest {

  private static final String SERVER_DIRECTORY = "./target/dbfactory";
  private OServer server;

  private OrientDB factory;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(
        getClass().getClassLoader().getResourceAsStream("com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();

    factory = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());
  }

  @Test
  public void createAndUseRemoteDatabase() {
    if (!factory.exists("test"))
      factory.create("test", ODatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
  }

  //@Test(expected = OStorageExistsException.class)
  //TODO: Uniform database exist exceptions
  @Test(expected = OStorageException.class)
  public void doubleCreateRemoteDatabase() {
    factory.create("test", ODatabaseType.MEMORY);
    factory.create("test", ODatabaseType.MEMORY);
  }

  @Test
  public void createDropRemoteDatabase() {
    factory.create("test", ODatabaseType.MEMORY);
    assertTrue(factory.exists("test"));
    factory.drop("test");
    assertFalse(factory.exists("test"));
  }

  @Test
  public void testPool() {
    if (!factory.exists("test"))
      factory.create("test", ODatabaseType.MEMORY);

    ODatabasePool pool = new ODatabasePool(factory, "test", "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
    pool.close();
  }

  @Test
  public void testMultiThread() {
    if (!factory.exists("test"))
      factory.create("test", ODatabaseType.MEMORY);

    ODatabasePool pool = new ODatabasePool(factory, "test", "admin", "admin");

    //do a query and assert on other thread
    Runnable acquirer = () -> {

      ODatabaseDocument db = pool.acquire();

      try {
        assertThat(db.isActiveOnCurrentThread()).isTrue();

        List<ODocument> res = db.query(new OSQLSynchQuery<>("SELECT * FROM OUser"));

        assertThat(res).hasSize(3);

      } finally {

        db.close();
      }

    };

    //spawn 20 threads
    List<CompletableFuture<Void>> futures = IntStream.range(0, 19).boxed().map(i -> CompletableFuture.runAsync(acquirer))
        .collect(Collectors.toList());

    futures.forEach(cf -> cf.join());

    pool.close();
  }

  @Test
  public void testListDatabases() {
    assertEquals(factory.list().size(), 0);
    factory.create("test", ODatabaseType.MEMORY);
    List<String> databases = factory.list();
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
  }

  @Test
  public void testCopyOpenedDatabase() {
    factory.create("test", ODatabaseType.MEMORY);
    ODatabaseDocument db1;
    try (ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) factory.open("test", "admin", "admin")) {
      db1 = db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
  }

  @After
  public void after() {
    for (String db : factory.list()) {
      factory.drop(db);
    }

    factory.close();
    server.shutdown();

    Orient.instance().shutdown();
    OFileUtils.deleteRecursively(new File(SERVER_DIRECTORY));
    Orient.instance().startup();
  }
}
