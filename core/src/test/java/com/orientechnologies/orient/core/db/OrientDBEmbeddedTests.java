package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Ignore;
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
  public void createAndUseEmbeddedDatabase() {
    OrientDB orientDb = OrientDB.embedded(".", null);
    // OrientDB orientDb = OrientDB.fromUrl("local:.", null);

    if (!orientDb.exists("test", "", ""))
      orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);

    ODatabaseDocument db = orientDb.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    orientDb.close();

  }

  @Test(expected = OStorageExistsException.class)
  public void testEmbeddedDoubleCreate() {
    OrientDB orientDb = OrientDB.embedded(".", null);
    try {
      orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
      orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
    } finally {
      orientDb.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    OrientDB orientDb = OrientDB.embedded(".", null);
    // OrientDB orientDb = OrientDB.fromUrl("remote:localhost", null);
    try {
      orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
      assertTrue(orientDb.exists("test", "", ""));
      orientDb.drop("test", "", "");
      assertFalse(orientDb.exists("test", "", ""));
    } finally {
      orientDb.close();
    }
  }

  @Test
  public void testPool() {
    OrientDB orientDb = OrientDB.embedded(".", null);
    // OrientDB orientDb = OrientDB.fromUrl("local:.", null);

    if (!orientDb.exists("test", "", ""))
      orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);

    ODatabasePool pool = orientDb.openPool("test", "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument());
    db.close();
    pool.close();
    orientDb.close();
  }

  @Test
  public void testMultiThread() {

    OrientDB orientDb = OrientDB.embedded(".", null);
    // OrientDB orientDb = OrientDB.fromUrl("local:.", null);

    if (!orientDb.exists("test", "", ""))
      orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);

    ODatabasePool pool = orientDb.openPool("test", "admin", "admin");

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
    OrientDB orientDb = OrientDB.embedded(".", null);
    // OrientDB orientDb = OrientDB.fromUrl("local:.", null);
    assertEquals(orientDb.listDatabases("", "").size(), 0);
    orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
    Set<String> databases = orientDb.listDatabases("", "");
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
  }

  @Test
  public void testRegisterDatabase() {
    OrientDBEmbedded orientDb = (OrientDBEmbedded) OrientDB.embedded(".", null);
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
    OrientDB orientDb = OrientDB.embedded(".", null);
    // OrientDB orientDb = OrientDB.fromUrl("local:.", null);

    orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
    ODatabaseDocument db1;
    try (ODatabaseDocumentInternal db = (ODatabaseDocumentInternal) orientDb.open("test", "admin", "admin")) {
      db1 = db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
    orientDb.close();
  }

  @Test
  public void testOrientDBDatabaseOnlyMemory() {
    OrientDB orientDb = OrientDB.embedded("", null);
    // OrientDB orientDb = OrientDB.fromUrl("local:.", null);

    orientDb.create("test", "", "", OrientDB.DatabaseType.MEMORY);
    ODatabaseDocument db = orientDb.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    orientDb.close();
  }

  @Test(expected = ODatabaseException.class)
  public void testOrientDBDatabaseOnlyMemoryFailPlocal() {
    try (OrientDB orientDb = OrientDB.embedded("", null)) {
      // OrientDB orientDb = OrientDB.fromUrl("local:.", null);

      orientDb.create("test", "", "", OrientDB.DatabaseType.PLOCAL);
    }
  }
}
