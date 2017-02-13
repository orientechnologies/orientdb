package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import org.junit.Test;

import java.util.List;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * Created by tglman on 13/01/17.
 */
public class OrientDBObjectTests {

  @Test
  public void createAndUseEmbeddedDatabase() {
    OrientDBObject factory = new OrientDBObject("embedded:.", null);

    if (!factory.exists("test"))
      factory.create("test", ODatabaseType.MEMORY);

    ODatabaseObject db = factory.open("test", "admin", "admin");
    db.close();
    factory.close();

  }

  @Test(expected = OStorageExistsException.class)
  public void testEmbeddedDoubleCreate() {
    OrientDBObject factory = new OrientDBObject("embedded:.", null);
    try {
      factory.create("test", ODatabaseType.MEMORY);
      factory.create("test", ODatabaseType.MEMORY);
    } finally {
      factory.close();
    }
  }

  @Test
  public void createDropEmbeddedDatabase() {
    OrientDBObject factory = new OrientDBObject("embedded:.", null);
    try {
      factory.create("test", ODatabaseType.MEMORY);
      assertTrue(factory.exists("test"));
      factory.drop("test");
      assertFalse(factory.exists("test"));
    } finally {
      factory.close();
    }
  }

  @Test
  public void testPool() {
    OrientDBObject factory = new OrientDBObject("embedded:.", null);

    if (!factory.exists("test"))
      factory.create("test", ODatabaseType.MEMORY);

    ODatabaseObjectPool pool = new ODatabaseObjectPool(factory, "test", "admin", "admin");
    ODatabaseObject db = pool.acquire();
    db.close();
    pool.close();
    factory.close();
  }

  @Test
  public void testListDatabases() {
    OrientDBObject factory = new OrientDBObject("embedded:.", null);
    assertEquals(factory.list().size(), 0);
    factory.create("test", ODatabaseType.MEMORY);
    List<String> databases = factory.list();
    assertEquals(databases.size(), 1);
    assertTrue(databases.contains("test"));
  }

}
