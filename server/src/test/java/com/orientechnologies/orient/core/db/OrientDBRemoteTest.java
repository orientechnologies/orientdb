package com.orientechnologies.orient.core.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OStorageException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 06/07/16. */
public class OrientDBRemoteTest {

  private static final String SERVER_DIRECTORY = "./target/dbfactory";
  private OServer server;

  private OrientDB factory;

  @Before
  public void before() throws Exception {
    ODatabaseRecordThreadLocal.instance().remove();

    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.setServerRootDirectory(SERVER_DIRECTORY);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();

    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 300_000)
            .build();
    factory = new OrientDB("remote:localhost", "root", "root", config);
  }

  @Test
  public void createAndUseRemoteDatabase() {
    if (!factory.exists("test")) {
      factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    }

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
  }

  // @Test(expected = OStorageExistsException.class)
  // TODO: Uniform database exist exceptions
  @Test(expected = OStorageException.class)
  public void doubleCreateRemoteDatabase() {
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
  }

  @Test
  public void createDropRemoteDatabase() {
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    assertTrue(factory.exists("test"));
    factory.drop("test");
    assertFalse(factory.exists("test"));
  }

  @Test
  public void testPool() {
    if (!factory.exists("test"))
      factory.execute("create database test memory users (admin identified by 'admin' role admin)");

    ODatabasePool pool = new ODatabasePool(factory, "test", "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
    pool.close();
  }

  @Test
  @Ignore
  public void testCachedPool() {
    if (!factory.exists("testdb"))
      factory.execute(
          "create database testdb memory users (admin identified by 'admin' role admin, reader identified by 'reader' role reader, writer identified by 'writer' role writer)");

    ODatabasePool poolAdmin1 = factory.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolAdmin2 = factory.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolReader1 = factory.cachedPool("testdb", "reader", "reader");
    ODatabasePool poolReader2 = factory.cachedPool("testdb", "reader", "reader");

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    ODatabasePool poolWriter1 = factory.cachedPool("testdb", "writer", "writer");
    ODatabasePool poolWriter2 = factory.cachedPool("testdb", "writer", "writer");
    assertEquals(poolWriter1, poolWriter2);

    ODatabasePool poolAdmin3 = factory.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolAdmin1, poolAdmin3);

    poolAdmin1.close();
    poolReader1.close();
    poolWriter1.close();
  }

  @Test
  public void testCachedPoolFactoryCleanUp() throws Exception {
    if (!factory.exists("testdb"))
      factory.execute(
          "create database testdb memory users (admin identified by 'admin' role admin)");

    ODatabasePool poolAdmin1 = factory.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolAdmin2 = factory.cachedPool("testdb", "admin", "admin");

    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(5_000);

    ODatabasePool poolAdmin3 = factory.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    poolAdmin3.close();
  }

  @Test
  @Ignore
  public void testMultiThread() {
    if (!factory.exists("test"))
      factory.execute(
          "create database test memory users (admin identified by 'admin' role admin, reader identified by 'reader' role reader, writer identified by 'writer' role writer)");

    ODatabasePool pool = new ODatabasePool(factory, "test", "admin", "admin");

    // do a query and assert on other thread
    Runnable acquirer =
        () -> {
          ODatabaseDocument db = pool.acquire();

          try {
            assertThat(db.isActiveOnCurrentThread()).isTrue();

            List<ODocument> res = db.query(new OSQLSynchQuery<>("SELECT * FROM OUser"));

            assertThat(res).hasSize(3);

          } finally {

            db.close();
          }
        };

    // spawn 20 threads
    List<CompletableFuture<Void>> futures =
        IntStream.range(0, 19)
            .boxed()
            .map(i -> CompletableFuture.runAsync(acquirer))
            .collect(Collectors.toList());

    futures.forEach(cf -> cf.join());

    pool.close();
  }

  @Test
  public void testListDatabases() {
    assertEquals(factory.list().size(), 0);
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    List<String> databases = factory.list();
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
  }

  @Test
  public void createDatabaseNoUsers() {
    factory.create(
        "noUser",
        ODatabaseType.MEMORY,
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build());
    try (ODatabaseSession session = factory.open("noUser", "root", "root")) {
      assertEquals(session.query("select from OUser").stream().count(), 0);
    }
  }

  @Test
  public void createDatabaseDefaultUsers() {
    factory.create(
        "noUser",
        ODatabaseType.MEMORY,
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, true)
            .build());
    try (ODatabaseSession session = factory.open("noUser", "root", "root")) {
      assertEquals(session.query("select from OUser").stream().count(), 3);
    }
  }

  @Test
  public void testCopyOpenedDatabase() {
    factory.execute("create database test memory users (admin identified by 'admin' role admin)");
    ODatabaseDocument db1;
    try (ODatabaseDocumentInternal db =
        (ODatabaseDocumentInternal) factory.open("test", "admin", "admin")) {
      db1 = db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
  }

  @Test
  public void testCreateDatabaseViaSQL() {
    String dbName = "testCreateDatabaseViaSQL";

    try (OResultSet result = factory.execute("create database " + dbName + " plocal")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(factory.exists(dbName));
    factory.drop(dbName);
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

    ODatabaseRecordThreadLocal.instance().remove();
  }
}
