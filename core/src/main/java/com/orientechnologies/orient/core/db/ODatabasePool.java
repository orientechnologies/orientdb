package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;

/**
 * Created by tglman on 08/02/17.
 */
public class ODatabasePool implements AutoCloseable {

  private final OrientDB              orientDb;
  private       ODatabasePoolInternal internal;
  private final boolean               autoclose;

  public ODatabasePool(OrientDB environment, String database, String user, String password) {
    orientDb = environment;
    autoclose = false;
    internal = orientDb.openPool(database, user, password);
  }

  public ODatabasePool(String environment, String database, String user, String password, OrientDBConfig configuration) {
    orientDb = new OrientDB(environment, configuration);
    autoclose = true;
    internal = orientDb.openPool(database, user, password);
  }

  public ODatabaseDocument acquire() {
    return internal.acquire();
  }

  @Override
  public void close() {
    internal.close();
    if (autoclose)
      orientDb.close();
  }
}
