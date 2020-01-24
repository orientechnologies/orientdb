package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.junit.Test;

import java.util.List;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBEmbeddedTests {

  @Test
  public void testCompatibleUrl() {
    try (OrientDB orientDb = new OrientDB("plocal:", OrientDBConfig.defaultConfig())) {
    }
    try (OrientDB orientDb = new OrientDB("memory:", OrientDBConfig.defaultConfig())) {
    }
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWrongUrlFalure() {
    try (OrientDB wrong = new OrientDB("wrong", OrientDBConfig.defaultConfig())) {
    }
  }

  @Test
  public void createAndUseEmbeddedDatabase() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {

      if (!orientDb.exists("createAndUseEmbeddedDatabase"))
        orientDb.create("createAndUseEmbeddedDatabase", ODatabaseType.MEMORY);

      ODatabaseSession db = orientDb.open("createAndUseEmbeddedDatabase", "admin", "admin");
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

    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {

      if (!orientDb.exists("testMultiThread"))
        orientDb.create("testMultiThread", ODatabaseType.MEMORY);

      ODatabasePool pool = new ODatabasePool(orientDb, "testMultiThread", "admin", "admin");

      //do a query and assert on other thread
      Runnable acquirer = () -> {

        ODatabaseSession db = pool.acquire();

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
    OrientDBEmbedded orientDb = (OrientDBEmbedded) new OrientDB("embedded:", OrientDBConfig.defaultConfig()).getInternal();
    assertEquals(orientDb.listDatabases("", "").size(), 0);
    orientDb.initCustomStorage("database1", "./target/databases/database1", "", "");
    try (ODatabaseSession db = orientDb.open("database1", "admin", "admin")) {
      assertEquals("database1", db.getName());
    }
    orientDb.initCustomStorage("database2", "./target/databases/database2", "", "");

    try (ODatabaseSession db = orientDb.open("database2", "admin", "admin")) {
      assertEquals("database2", db.getName());
    }
    orientDb.drop("database1", null, null);
    orientDb.drop("database2", null, null);
    orientDb.close();
  }

  @Test
  public void testCopyOpenedDatabase() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {

      orientDb.create("testCopyOpenedDatabase", ODatabaseType.MEMORY);
      ODatabaseSession db1;
      try (ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) orientDb.open("testCopyOpenedDatabase", "admin", "admin")) {
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

    OrientDB orientDb = new OrientDB("embedded:./target", OrientDBConfig.defaultConfig());
    orientDb.createIfNotExists("some", ODatabaseType.PLOCAL);

    orientDb.close();

    try {

      ODatabasePool pool = new ODatabasePool("embedded:./target/some", "admin", "admin");
      pool.close();

    } finally {

    }

  }

  @Test
  public void testClosePool() {
    ODatabasePool pool = new ODatabasePool("embedded:./target/some", "admin", "admin");
    assertFalse(pool.isClosed());

    pool.close();

    assertTrue(pool.isClosed());
  }

  @Test
  public void testPoolFactory() {
    OrientDBConfig config = OrientDBConfig.builder().addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2).build();
    OrientDB orientDB = new OrientDB("embedded:testdb", config);
    orientDB.createIfNotExists("testdb", ODatabaseType.MEMORY);

    ODatabasePool poolAdmin1 = orientDB.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolAdmin2 = orientDB.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolReader1 = orientDB.cachedPool("testdb", "reader", "reader");
    ODatabasePool poolReader2 = orientDB.cachedPool("testdb", "reader", "reader");

    assertEquals(poolAdmin1, poolAdmin2);
    assertEquals(poolReader1, poolReader2);
    assertNotEquals(poolAdmin1, poolReader1);

    ODatabasePool poolWriter1 = orientDB.cachedPool("testdb", "writer", "writer");
    ODatabasePool poolWriter2 = orientDB.cachedPool("testdb", "writer", "writer");
    assertEquals(poolWriter1, poolWriter2);

    ODatabasePool poolAdmin3 = orientDB.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolAdmin1, poolAdmin3);

    orientDB.close();
  }

  @Test
  public void testPoolFactoryCleanUp() throws Exception {
    OrientDBConfig config = OrientDBConfig.builder().addConfig(OGlobalConfiguration.DB_CACHED_POOL_CAPACITY, 2)
        .addConfig(OGlobalConfiguration.DB_CACHED_POOL_CLEAN_UP_TIMEOUT, 1_000).build();
    OrientDB orientDB = new OrientDB("embedded:testdb", config);
    orientDB.createIfNotExists("testdb", ODatabaseType.MEMORY);
    orientDB.createIfNotExists("testdb1", ODatabaseType.MEMORY);

    ODatabasePool poolNotUsed = orientDB.cachedPool("testdb1", "admin", "admin");
    ODatabasePool poolAdmin1 = orientDB.cachedPool("testdb", "admin", "admin");
    ODatabasePool poolAdmin2 = orientDB.cachedPool("testdb", "admin", "admin");

    assertFalse(poolAdmin1.isClosed());
    assertEquals(poolAdmin1, poolAdmin2);

    poolAdmin1.close();

    assertTrue(poolAdmin1.isClosed());

    Thread.sleep(3_000);

    ODatabasePool poolAdmin3 = orientDB.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolAdmin1, poolAdmin3);
    assertFalse(poolAdmin3.isClosed());

    ODatabasePool poolOther = orientDB.cachedPool("testdb", "admin", "admin");
    assertNotEquals(poolNotUsed, poolOther);
    assertTrue(poolNotUsed.isClosed());

    orientDB.close();
  }

  @Test
  public void testOpenKeepClean() {
    OrientDB orientDb = new OrientDB("embedded:./", OrientDBConfig.defaultConfig());
    try {
      orientDb.open("test", "admin", "admin");
    } catch (Exception e) {
      //ignore
    }
    assertFalse(orientDb.exists("test"));

    orientDb.close();
  }

  @Test
  public void testOrientDBDatabaseOnlyMemory() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    orientDb.create("test", ODatabaseType.MEMORY);
    ODatabaseSession db = orientDb.open("test", "admin", "admin");
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
    orientDb.close();
  }

  @Test(expected = ODatabaseException.class)
  public void testOrientDBDatabaseOnlyMemoryFailPlocal() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.create("test", ODatabaseType.PLOCAL);
    }
  }

  @Test
  public void createForceCloseOpen() throws InterruptedException {
    try (OrientDB orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig())) {
      orientDB.create("testCreateForceCloseOpen", ODatabaseType.PLOCAL);
      ((OrientDBEmbedded) orientDB.getInternal()).forceDatabaseClose("test");
      ODatabaseSession db1 = orientDB.open("testCreateForceCloseOpen", "admin", "admin");
      assertFalse(db1.isClosed());
      db1.close();
      orientDB.drop("testCreateForceCloseOpen");
    }
  }

  @Test
  public void autoClose() throws InterruptedException {
    OrientDB orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    OrientDBEmbedded embedded = ((OrientDBEmbedded) OrientDBInternal.extract(orientDB));
    embedded.initAutoClose(3000);
    orientDB.create("test", ODatabaseType.PLOCAL);
    ODatabaseSession db1 = orientDB.open("test", "admin", "admin");
    assertFalse(db1.isClosed());
    db1.close();
    assertNotNull(embedded.getStorage("test"));
    Thread.sleep(4100);
    assertNull(embedded.getStorage("test"));
    orientDB.drop("test");
    orientDB.close();
  }

  @Test(expected = ODatabaseException.class)
  public void testOpenNotExistDatabase() {
    try (OrientDB orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig())) {
      orientDB.open("testOpenNotExistDatabase", "two", "three");
    }
  }

  @Test
  public void testExecutor() throws ExecutionException, InterruptedException {

    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {

      orientDb.create("testExecutor", ODatabaseType.MEMORY);
      OrientDBInternal internal = OrientDBInternal.extract(orientDb);
      Future<Boolean> result = internal.execute("testExecutor", "admin", (session) -> {
        if (session.isClosed() && session.getUser() == null) {
          return false;
        }
        return true;
      });

      assertTrue(result.get());
    }
  }

  @Test
  public void testExecutorNoAuthorization() throws ExecutionException, InterruptedException {

    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.create("testExecutorNoAuthorization", ODatabaseType.MEMORY);
      OrientDBInternal internal = OrientDBInternal.extract(orientDb);
      Future<Boolean> result = internal.executeNoAuthorization("testExecutorNoAuthorization", (session) -> {
        if (session.isClosed() && session.getUser() != null) {
          return false;
        }
        return true;
      });

      assertTrue(result.get());
    }

  }

  @Test
  public void testScheduler() throws InterruptedException {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    OrientDBInternal internal = OrientDBInternal.extract(orientDb);
    CountDownLatch latch = new CountDownLatch(2);
    internal.schedule(new TimerTask() {
      @Override
      public void run() {
        latch.countDown();

      }
    }, 10, 10);

    assertTrue(latch.await(80, TimeUnit.MILLISECONDS));

    CountDownLatch once = new CountDownLatch(1);
    internal.scheduleOnce(new TimerTask() {
      @Override
      public void run() {
        once.countDown();

      }
    }, 10);

    assertTrue(once.await(80, TimeUnit.MILLISECONDS));
  }

  @Test
  public void testUUID() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.create("testUUID", ODatabaseType.MEMORY);
      ODatabaseSession session = orientDb.open("testUUID", "admin", "admin");
      assertNotNull(((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage()).getUuid());
      session.close();
    }
  }

  @Test
  public void testPersistentUUID() {
    OrientDB orientDb = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    orientDb.create("testPersistentUUID", ODatabaseType.PLOCAL);
    ODatabaseSession session = orientDb.open("testPersistentUUID", "admin", "admin");
    UUID uuid = ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage()).getUuid();
    assertNotNull(uuid);
    session.close();
    orientDb.close();
    OrientDB orientDb1 = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    ODatabaseSession session1 = orientDb1.open("testPersistentUUID", "admin", "admin");
    assertEquals(uuid, ((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session1).getStorage()).getUuid());
    session1.close();
    orientDb1.drop("testPersistentUUID");
    orientDb1.close();

  }

}
