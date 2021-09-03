package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;

/** Created by tglman on 13/01/17. */
public class ODatabaseObjectPool implements AutoCloseable {
  private ODatabasePool pool;

  public ODatabaseObjectPool(
      OrientDBObject environment, String database, String user, String password) {
    this(environment, database, user, password, OrientDBConfig.defaultConfig());
  }

  public ODatabaseObjectPool(
      OrientDBObject environment,
      String database,
      String user,
      String password,
      OrientDBConfig configuration) {
    pool = new ODatabasePool(environment.getOrientDB(), database, user, password, configuration);
  }

  public ODatabaseObjectPool(String url, String user, String password) {
    this(url, user, password, OrientDBConfig.defaultConfig());
  }

  public ODatabaseObjectPool(
      String url, String user, String password, OrientDBConfig configuration) {
    pool = new ODatabasePool(url, user, password, configuration);
  }

  public ODatabaseObjectPool(String environment, String database, String user, String password) {
    this(environment, database, user, password, OrientDBConfig.defaultConfig());
  }

  public ODatabaseObjectPool(
      String environment,
      String database,
      String user,
      String password,
      OrientDBConfig configuration) {
    pool = new ODatabasePool(environment, database, user, password, configuration);
  }

  public ODatabaseObject acquire() {
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) pool.acquire());
  }

  public void close() {
    this.pool.close();
  }
}
