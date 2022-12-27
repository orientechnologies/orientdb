package com.orientechnologies.orient.core.db;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageDoesNotExistException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.List;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;

/** Created by tglman on 08/04/16. */
public class OrientDBEmbeddedTests {
  @Test
  public void testCompatibleUrl() {
    try (OrientDB orientDb = new OrientDB("plocal:", OrientDBConfig.defaultConfig())) {}
    try (OrientDB orientDb = new OrientDB("memory:", OrientDBConfig.defaultConfig())) {}
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongUrlFalure() {
    try (OrientDB wrong = new OrientDB("wrong", OrientDBConfig.defaultConfig())) {}
  }

  @Test
  public void createAndUseEmbeddedDatabase() {
    try (final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "createAndUseEmbeddedDatabase", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabaseSession db =
          orientDb.open(
              "createAndUseEmbeddedDatabase", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
      db.close();
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testEmbeddedDoubleCreate() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    try {
      orientDb.create("test", ODatabaseType.MEMORY);
      orientDb.create("test", ODatabaseType.MEMORY);
    } finally {
      orientDb.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    try {
      orientDb.create("test", ODatabaseType.MEMORY);
      assertTrue(orientDb.exists("test"));
      orientDb.drop("test");
      assertFalse(orientDb.exists("test"));
    } finally {
      orientDb.close();
    }
  }

  @Test
  public void testMultiThread() {
    try (final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "testMultiThread", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabasePool pool =
          new ODatabasePool(
              orientDb, "testMultiThread", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

      // do a query and assert on other thread
      Runnable acquirer =
          () -> {
            ODatabaseSession db = pool.acquire();
            try {
              assertThat(db.isActiveOnCurrentThread()).isTrue();
              final List<ODocument> res = db.query(new OSQLSynchQuery<>("SELECT * FROM OUser"));
              assertThat(res).hasSize(1); // Only 'admin' created in this test
            } finally {
              db.close();
            }
          };

      // spawn 20 threads
      final List<CompletableFuture<Void>> futures =
          IntStream.range(0, 19)
              .boxed()
              .map(i -> CompletableFuture.runAsync(acquirer))
              .collect(Collectors.toList());

      futures.forEach(cf -> cf.join());

      pool.close();
    }
  }

  @Test
  public void testListDatabases() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    // OrientDBInternal orientDb = OrientDBInternal.fromUrl("local:.", null);
    assertEquals(orientDb.list().size(), 0);
    orientDb.create("test", ODatabaseType.MEMORY);
    List<String> databases = orientDb.list();
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
    orientDb.close();
  }

  @Test
  public void testRegisterDatabase() {
    final OrientDB orient = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orient.execute("create system user admin identified by 'admin' role root");
    final OrientDBEmbedded orientDb = (OrientDBEmbedded) orient.getInternal();
    assertEquals(orientDb.listDatabases("", "").size(), 0);
    orientDb.initCustomStorage("database1", "./target/databases/database1", "", "");
    try (final ODatabaseSession db = orientDb.open("database1", "admin", "admin")) {
      assertEquals("database1", db.getName());
    }
    orientDb.initCustomStorage("database2", "./target/databases/database2", "", "");

    try (final ODatabaseSession db = orientDb.open("database2", "admin", "admin")) {
      assertEquals("database2", db.getName());
    }
    orientDb.drop("database1", null, null);
    orientDb.drop("database2", null, null);
    orientDb.close();
  }

  @Test
  public void testCopyOpenedDatabase() {
    try (final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "testCopyOpenedDatabase", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      ODatabaseSession db1;
      try (ODatabaseDocumentInternal db =
          (ODatabaseDocumentInternal)
              orientDb.open(
                  "testCopyOpenedDatabase", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
        db1 = db.copy();
      }
      db1.activateOnCurrentThread();
      assertFalse(db1.isClosed());
      db1.close();
    }
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseCreate() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.close();
    orientDb.create("test", ODatabaseType.MEMORY);
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseOpen() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.close();
    orientDb.open("testUseAfterCloseOpen", "", "");
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseList() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.close();
    orientDb.list();
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseExists() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.close();
    orientDb.exists("");
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseOpenPoolInternal() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.close();
    orientDb.openPool("", "", "", OrientDBConfig.defaultConfig());
  }

  @Test(expected = ODatabaseException.class)
  public void testUseAfterCloseDrop() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.close();
    orientDb.drop("");
  }

  @Test
  public void testPoolByUrl() {
    final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "some", "embedded:./target", OCreateDatabaseUtil.TYPE_PLOCAL);
    orientDb.close();

    final ODatabasePool pool =
        new ODatabasePool(
            "embedded:./target/some", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    pool.close();
  }

  @Test
  public void testDropTL() {
    final OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!orientDb.exists("some")) {
      orientDb.execute(
          "create database "
              + "some"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!orientDb.exists("some1")) {
      orientDb.execute(
          "create database "
              + "some1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    final ODatabaseDocument db =
        orientDb.open("some", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    orientDb.drop("some1");
    db.close();
    orientDb.close();
  }

  @Test
  public void testClosePool() {
    final ODatabasePool pool =
        new ODatabasePool(
            "embedded:./target/some",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertFalse(pool.isClosed());
    pool.close();
    assertTrue(pool.isClosed());
  }

  @Test
  public void testPoolFactory() {
    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    OrientDB orientDB = new OrientDB("embedded:", config);
    if (!orientDB.exists("testdb")) {
      orientDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin, reader identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role reader, writer identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role writer)");
    }
    ODatabasePool poolAdmin1 =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin2 =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolReader1 =
        orientDB.cachedPool("testdb", "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolReader2 =
        orientDB.cachedPool("testdb", "reader", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    ODatabasePool poolWriter1 =
        orientDB.cachedPool("testdb", "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolWriter2 =
        orientDB.cachedPool("testdb", "writer", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(poolWriter1, poolWriter2);

    ODatabasePool poolAdmin3 =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);

    orientDB.close();
  }

  @Test
  public void testPoolFactoryCleanUp() throws Exception {
    OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 1_000)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    OrientDB orientDB = new OrientDB("embedded:", config);
    if (!orientDB.exists("testdb")) {
      orientDB.execute(
          "create database "
              + "testdb"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    if (!orientDB.exists("testdb1")) {
      orientDB.execute(
          "create database "
              + "testdb1"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    ODatabasePool poolNotUsed =
        orientDB.cachedPool(
            "testdb1",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin1 =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabasePool poolAdmin2 =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(3_000);

    ODatabasePool poolAdmin3 =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    ODatabasePool poolOther =
        orientDB.cachedPool(
            "testdb",
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD,
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    assertNotEquals(poolNotUsed, poolOther);
    assertTrue(poolNotUsed.isClosed());

    orientDB.close();
  }

  @Test
  public void testInvalidatePoolCache() {
    final OrientDBConfig config =
        OrientDBConfig.builder()
            .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
            .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
            .build();
    final OrientDB orientDB = new OrientDB("embedded:", config);
    orientDB.execute(
        "create database "
            + "testdb"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");

    ODatabasePool poolAdmin1 =
        orientDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabasePool poolAdmin2 =
        orientDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    assertEquals(poolAdmin1, poolAdmin2);

    orientDB.invalidateCachedPools();

    poolAdmin1 = orientDB.cachedPool("testdb", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertNotEquals(poolAdmin2, poolAdmin1);
  }

  @Test
  public void testOpenKeepClean() {
    OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    try {
      orientDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    } catch (Exception e) {
      // ignore
    }
    assertFalse(orientDb.exists("test"));

    orientDb.close();
  }

  @Test
  public void testOrientDBDatabaseOnlyMemory() {
    final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabaseSession db =
        orientDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
    orientDb.close();
  }

  @Test(expected = ODatabaseException.class)
  public void testOrientDBDatabaseOnlyMemoryFailPlocal() {
    try (OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build())) {
      orientDb.create("test", ODatabaseType.PLOCAL);
    }
  }

  @Test
  public void createForceCloseOpen() throws InterruptedException {
    try (final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase(
            "testCreateForceCloseOpen", "embedded:./target/", OCreateDatabaseUtil.TYPE_PLOCAL)) {
      orientDB.getInternal().forceDatabaseClose("test");
      ODatabaseSession db1 =
          orientDB.open(
              "testCreateForceCloseOpen", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertFalse(db1.isClosed());
      db1.close();
      orientDB.drop("testCreateForceCloseOpen");
    }
  }

  @Test
  @Ignore
  public void autoClose() throws InterruptedException {
    OrientDB orientDB =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    OrientDBEmbedded embedded = ((OrientDBEmbedded) OrientDBInternal.extract(orientDB));
    embedded.initAutoClose(3000);
    orientDB.execute(
        "create database "
            + "test"
            + " "
            + "plocal"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    ODatabaseSession db1 = orientDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertFalse(db1.isClosed());
    db1.close();
    assertNotNull(embedded.getStorage("test"));
    Thread.sleep(4100);
    assertNull(embedded.getStorage("test"));
    orientDB.drop("test");
    orientDB.close();
  }

  @Test(expected = OStorageDoesNotExistException.class)
  public void testOpenNotExistDatabase() {
    try (OrientDB orientDB =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build())) {
      orientDB.open("testOpenNotExistDatabase", "two", "three");
    }
  }

  @Test
  public void testExecutor() throws ExecutionException, InterruptedException {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.create("testExecutor", ODatabaseType.MEMORY);
      OrientDBInternal internal = OrientDBInternal.extract(orientDb);
      Future<Boolean> result =
          internal.execute(
              "testExecutor",
              "admin",
              (session) -> {
                return !session.isClosed() || session.getUser() != null;
              });

      assertTrue(result.get());
    }
  }

  @Test
  public void testExecutorNoAuthorization() throws ExecutionException, InterruptedException {

    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.create("testExecutorNoAuthorization", ODatabaseType.MEMORY);
      OrientDBInternal internal = OrientDBInternal.extract(orientDb);
      Future<Boolean> result =
          internal.executeNoAuthorization(
              "testExecutorNoAuthorization",
              (session) -> {
                return !session.isClosed() || session.getUser() == null;
              });

      assertTrue(result.get());
    }
  }

  @Test
  public void testScheduler() throws InterruptedException {
    OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    OrientDBInternal internal = OrientDBInternal.extract(orientDb);
    CountDownLatch latch = new CountDownLatch(2);
    internal.schedule(
        new TimerTask() {
          @Override
          public void run() {
            latch.countDown();
          }
        },
        10,
        10);

    assertTrue(latch.await(80, TimeUnit.MILLISECONDS));

    CountDownLatch once = new CountDownLatch(1);
    internal.scheduleOnce(
        new TimerTask() {
          @Override
          public void run() {
            once.countDown();
          }
        },
        10);

    assertTrue(once.await(80, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testUUID() {
    try (final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "testUUID", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY)) {
      final ODatabaseSession session =
          orientDb.open("testUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      assertNotNull(
          ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage())
              .getUuid());
      session.close();
    }
  }

  @Test
  public void testPersistentUUID() {
    final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase(
            "testPersistentUUID", "embedded:./target/", OCreateDatabaseUtil.TYPE_PLOCAL);
    final ODatabaseSession session =
        orientDb.open("testPersistentUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    UUID uuid =
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage()).getUuid();
    assertNotNull(uuid);
    session.close();
    orientDb.close();

    OrientDB orientDb1 =
        new OrientDB(
            "embedded:./target/",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    ODatabaseSession session1 =
        orientDb1.open("testPersistentUUID", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    assertEquals(
        uuid,
        ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session1).getStorage())
            .getUuid());
    session1.close();
    orientDb1.drop("testPersistentUUID");
    orientDb1.close();
  }

  @Test
  public void testCreateDatabaseViaSQL() {
    String dbName = "testCreateDatabaseViaSQL";
    OrientDB orientDb = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    try (OResultSet result = orientDb.execute("create database " + dbName + " plocal")) {
      Assert.assertTrue(result.hasNext());
      OResult item = result.next();
      Assert.assertEquals(true, item.getProperty("created"));
    }
    Assert.assertTrue(orientDb.exists(dbName));

    orientDb.drop(dbName);
    orientDb.close();
  }

  @Test
  public void testCreateDatabaseViaSQLWithUsers() {
    OrientDB orientDB =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    orientDB.execute(
        "create database test memory users(admin identified by 'adminpwd' role admin)");
    try (ODatabaseSession session = orientDB.open("test", "admin", "adminpwd")) {}

    orientDB.close();
  }

  @Test
  public void testCreateDatabaseViaSQLIfNotExistsWithUsers() {
    final OrientDB orientDB =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    orientDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role admin)");

    orientDB.execute(
        "create database test memory if not exists users(admin identified by 'adminpwd' role admin)");

    try (ODatabaseSession session = orientDB.open("test", "admin", "adminpwd")) {}

    orientDB.close();
  }
}
