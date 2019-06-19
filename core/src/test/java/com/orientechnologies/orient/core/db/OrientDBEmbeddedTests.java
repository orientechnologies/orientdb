package com.orientechnologies.orient.core.db;

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
    new OrientDB("wrong", OrientDBConfig.defaultConfig());
  }

  @Test
  public void createAndUseEmbeddedDatabase() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    if (!orientDb.exists("test"))
      orientDb.create("test", ODatabaseType.MEMORY);

    ODatabaseSession db = orientDb.open("test", "admin", "admin");
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
    orientDb.close();

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

    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    if (!orientDb.exists("test"))
      orientDb.create("test", ODatabaseType.MEMORY);

    ODatabasePool pool = new ODatabasePool(orientDb, "test", "admin", "admin");

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
    orientDb.close();

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
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    orientDb.create("test", ODatabaseType.MEMORY);
    ODatabaseSession db1;
    try (ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) orientDb.open("test", "admin", "admin")) {
      db1 = db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
    orientDb.close();
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
    orientDb.open("test", "", "");
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
    ODatabasePool pool = new ODatabasePool("embedded:./target/some", "admin", "admin");
    pool.close();
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
    OrientDB orientDB = new OrientDB("embedded:./target/", OrientDBConfig.defaultConfig());
    orientDB.create("test", ODatabaseType.PLOCAL);
    ((OrientDBEmbedded) orientDB.getInternal()).forceDatabaseClose("test");
    ODatabaseSession db1 = orientDB.open("test", "admin", "admin");
    assertFalse(db1.isClosed());
    db1.close();
    orientDB.drop("test");
    orientDB.close();
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
      orientDB.open("one", "two", "three");
    }
  }

  @Test
  public void testExecutor() throws ExecutionException, InterruptedException {

    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    orientDb.create("test", ODatabaseType.MEMORY);
    OrientDBInternal internal = OrientDBInternal.extract(orientDb);
    Future<Boolean> result = internal.execute("test", "admin", (session) -> {
      if (session.isClosed() && session.getUser() == null) {
        return false;
      }
      return true;
    });

    assertTrue(result.get());
    orientDb.close();

  }

  @Test
  public void testExecutorNoAuthorization() throws ExecutionException, InterruptedException {

    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    orientDb.create("test", ODatabaseType.MEMORY);
    OrientDBInternal internal = OrientDBInternal.extract(orientDb);
    Future<Boolean> result = internal.executeNoAuthorization("test", (session) -> {
      if (session.isClosed() && session.getUser() != null) {
        return false;
      }
      return true;
    });

    assertTrue(result.get());
    orientDb.close();

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
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());
    orientDb.create("test", ODatabaseType.MEMORY);
    ODatabaseSession session = orientDb.open("test", "admin", "admin");
    assertNotNull(((OAbstractPaginatedStorage) ((ODatabaseDocumentInternal) session).getStorage()).getUuid());
    session.close();
    orientDb.close();
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
