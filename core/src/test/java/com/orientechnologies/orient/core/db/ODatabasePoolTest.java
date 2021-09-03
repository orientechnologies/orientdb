package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

public class ODatabasePoolTest {
  @Test
  public void testPool() {
    final OrientDB orientDb =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    final ODatabasePool pool =
        new ODatabasePool(orientDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    final ODatabaseDocument db = pool.acquire();
    db.save(new ODocument(), db.getClusterNameById(db.getDefaultClusterId()));
    db.close();
    pool.close();
    orientDb.close();
  }

  @Test
  public void testPoolCloseTx() {
    final OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    if (!orientDb.exists("test")) {
      orientDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    final ODatabasePool pool =
        new ODatabasePool(orientDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabaseDocument db = pool.acquire();
    db.createClass("Test");
    db.begin();
    db.save(new ODocument("Test"));
    db.close();
    db = pool.acquire();
    assertEquals(db.countClass("Test"), 0);
    db.close();
    pool.close();
    orientDb.close();
  }

  @Test
  public void testPoolDoubleClose() {
    final OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());

    if (!orientDb.exists("test")) {
      orientDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }

    final ODatabasePool pool =
        new ODatabasePool(orientDb, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    ODatabaseDocument db = pool.acquire();
    db.close();
    db.close();
    orientDb.close();
  }
}
