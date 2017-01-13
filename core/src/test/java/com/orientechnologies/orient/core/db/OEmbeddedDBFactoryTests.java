package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
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
public class OEmbeddedDBFactoryTests {

  @Test
  public void createAndUseEmbeddedDatabase() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    // OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    if (!factory.exists("test", "", ""))
      factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    factory.close();

  }

  @Test(expected = OStorageExistsException.class)
  public void testEmbeddedDoubleCreate() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    try {
      factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);
      factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);
    } finally {
      factory.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    // OrientDBFactory factory = OrientDBFactory.fromUrl("remote:localhost", null);
    try {
      factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);
      assertTrue(factory.exists("test", "", ""));
      factory.drop("test", "", "");
      assertFalse(factory.exists("test", "", ""));
    } finally {
      factory.close();
    }
  }

  @Test
  public void testPool() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    // OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    if (!factory.exists("test", "", ""))
      factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);

    ODatabasePool pool = factory.openPool("test", "admin", "admin");
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument());
    db.close();
    pool.close();
    factory.close();
  }

  @Test
  public void testListDatabases() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    // OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);
    assertEquals(factory.listDatabases("", "").size(), 0);
    factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);
    Set<String> databases = factory.listDatabases("", "");
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
  }
  
  @Test
  public void testRegisterDatabase() {
    OEmbeddedDBFactory factory = OrientDBFactory.embedded(".", null);
    assertEquals(factory.listDatabases("", "").size(), 0);
    factory.initCustomStorage("database1", "./target/databases/database1", "", "");
    try (ODatabaseDocument db = factory.open("database1", "admin", "admin")) {
      assertEquals("database1", db.getName());
    }
    factory.initCustomStorage("database2", "./target/databases/database2", "", "");

    try (ODatabaseDocument db = factory.open("database2", "admin", "admin")) {
      assertEquals("database2", db.getName());
    }
    factory.drop("database1", null, null);
    factory.drop("database2", null, null);
    factory.close();
  }
  

  @Test
  public void testCopyOpenedDatabase() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    // OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);
    ODatabaseDocument db1;
    try(ODatabaseDocumentInternal db = (ODatabaseDocumentInternal)factory.open("test", "admin", "admin")){
      db1 =db.copy();
    }
    db1.activateOnCurrentThread();
    assertFalse(db1.isClosed());
    db1.close();
  }

}
