package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by tglman on 08/04/16.
 */
public class OrientFactoryTests {

  public void createAndUseEmbeddedDatabase() {
    OrientFactory factory = OrientFactory.embedded(".", null);
    //    OrientFactory factory = OrientFactory.fromUrl("local:.", null);

    if (!factory.exist("test", "", ""))
      factory.create("test", "", "", OrientFactory.DatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "", "");
    db.save(new ODocument());
    db.close();
    factory.close();

  }

  public void createAndUseRemoteDatabase() {
    OrientFactory factory = OrientFactory.remote(new String[] { "localhost" }, null);
    //    OrientFactory factory = OrientFactory.fromUrl("remote:localhost", null);
    if (!factory.exist("test", "root", "root"))
      factory.create("test", "root", "root", OrientFactory.DatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    factory.close();

  }

  public void createAndUseDistributeEmbeddedDatabase() {
    OrientFactory factory = OrientFactory.join(new String[] { "localhost:3435" }, null);
    //    OrientFactory factory = OrientFactory.fromUrl("distributed:localhost:3435", null);
    //    WE need cluster root users ...
    if (!factory.exist("test", "root", "root"))
      factory.create("test", "root", "root", OrientFactory.DatabaseType.MEMORY);

    ODatabaseDocument db = factory.open("test", "admin", "admin");
    db.save(new ODocument());
    db.close();
    factory.close();

  }

  public void embeddedWithServer() {
    OEmbeddedFactory factory = OrientFactory.embedded(".", null);
    //OEmbeddedFactory factory = (OEmbeddedFactory)OrientFactory.fromUrl("local:.", null);
    factory.spawnServer(new Object()/*configuration*/);
  }

  public void testPool() {
    OrientFactory factory = OrientFactory.embedded(".", null);
    //    OrientFactory factory = OrientFactory.fromUrl("local:.", null);

    if (!factory.exist("test", "", ""))
      factory.create("test", "", "", OrientFactory.DatabaseType.MEMORY);

    Map<String, Object> settings = new HashMap<>();
    settings.put("min", 10);
    settings.put("max", 100);

    Pool<ODatabaseDocument> pool = factory.openPool("test", "admin", "admin", settings);
    ODatabaseDocument db = pool.acquire();
    db.save(new ODocument());
    db.close();
    pool.close();
    factory.close();
  }

}
