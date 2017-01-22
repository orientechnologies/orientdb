package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import java.util.Set;

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
    OrientDBEmbedded orientDb = OrientDB.embedded(".", null);
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
