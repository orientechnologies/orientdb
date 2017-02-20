package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

    ODatabaseDocument db = orientDb.open("test", "admin", "admin");
    db.save(new ODocument());
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
  public void testPool() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    if (!orientDb.exists("test"))
      orientDb.create("test", ODatabaseType.MEMORY);

    ODatabasePool pool = new ODatabasePool(orientDb, "test", "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument());
    db.close();
    pool.close();
    orientDb.close();
  }

  @Test
  public void testMultiThread() {

    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    if (!orientDb.exists("test"))
      orientDb.create("test", ODatabaseType.MEMORY);

    ODatabasePool pool = new ODatabasePool(orientDb, "test", "admin", "admin");

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
  }

  @Test
  public void testRegisterDatabase() {
    OrientDBEmbedded orientDb = (OrientDBEmbedded) new OrientDB("embedded:", OrientDBConfig.defaultConfig()).getInternal();
    assertEquals(orientDb.listDatabases("", "").size(), 0);
    orientDb.initCustomStorage("database1", "./target/databases/database1", "", "");
    try (ODatabaseDocument db = orientDb.open("database1", "admin", "admin")) {
      assertEquals("database1", db.getName());
    }
    orientDb.initCustomStorage("database2", "./target/databases/database2", "", "");

    try (ODatabaseDocument db = orientDb.open("database2", "admin", "admin")) {
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
    ODatabaseDocument db1;
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
  public void testOrientDBDatabaseOnlyMemory() {
    OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig());

    orientDb.create("test", ODatabaseType.MEMORY);
    ODatabaseDocument db = orientDb.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    orientDb.close();
  }

  @Test(expected = ODatabaseException.class)
  public void testOrientDBDatabaseOnlyMemoryFailPlocal() {
    try (OrientDB orientDb = new OrientDB("embedded:", OrientDBConfig.defaultConfig())) {
      orientDb.create("test", ODatabaseType.PLOCAL);
    }
  }
}
