package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.OStorageExistsException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientDBFactoryTests {

  @Test
  public void createAndUseEmbeddedDatabase() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    if (!factory.exist("test", "", ""))
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
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("remote:localhost", null);
    try {
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
      assertTrue(factory.exist("test", "root", "root"));
      factory.drop("test", "root", "root");
      assertFalse(factory.exist("test", "root", "root"));
    } finally {
      factory.close();
    }
  }

  public void testPool() {
    OrientDBFactory factory = OrientDBFactory.embedded(".", null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("local:.", null);

    if (!factory.exist("test", "", ""))
      factory.create("test", "", "", OrientDBFactory.DatabaseType.MEMORY);

    Map<String, Object> settings = new HashMap<>();
    settings.put("min", 10);
    settings.put("max", 100);

    OPool<ODatabaseDocument> pool = factory.openPool("test", "admin", "admin", settings);
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument());
    db.close();
    pool.close();
    factory.close();
  }

}
