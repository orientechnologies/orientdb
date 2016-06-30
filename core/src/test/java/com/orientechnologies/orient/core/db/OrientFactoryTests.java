package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientFactoryTests {

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

  @Test
  public void createAndUseRemoteDatabase() {
    OrientDBFactory factory = OrientDBFactory.remote(new String[] { "localhost" }, null);
    //    OrientDBFactory factory = OrientDBFactory.fromUrl("remote:localhost", null);
    if (!factory.exist("test", "root", "root"))
      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    factory.close();

  }

  public void createAndUseDistributeEmbeddedDatabase() {
    ////    OrientDBFactory factory = OrientDBFactory.join(new String[] { "localhost:3435" }, null);
    //    //    OrientDBFactory factory = OrientDBFactory.fromUrl("distributed:localhost:3435", null);
    //    //    WE need cluster root users ...
    //    if (!factory.exist("test", "root", "root"))
    //      factory.create("test", "root", "root", OrientDBFactory.DatabaseType.MEMORY);
    //
    //    ODatabaseDocument db = factory.open("test", "admin", "admin");
    //    db.save(new ODocument());
    //    db.close();
    //    factory.close();

  }

  public void embeddedWithServer() {
    OEmbeddedDBFactory factory = OrientDBFactory.embedded(".", null);
    //OEmbeddedDBFactory factory = (OEmbeddedDBFactory)OrientDBFactory.fromUrl("local:.", null);
    factory.spawnServer(new Object()/*configuration*/);
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
