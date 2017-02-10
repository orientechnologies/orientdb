package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import com.sun.org.omg.SendingContext.CodeBasePackage.URLHelper;

/**
 * Created by tglman on 08/02/17.
 */
public class ODatabasePool implements AutoCloseable {

  private final OrientDB              orientDb;
  private       ODatabasePoolInternal internal;
  private final boolean               autoclose;

  public ODatabasePool(OrientDB environment, String database, String user, String password) {
    this(environment, database, user, password, OrientDBConfig.defaultConfig());
  }

  public ODatabasePool(OrientDB environment, String database, String user, String password, OrientDBConfig configuration) {
    orientDb = environment;
    autoclose = false;
    internal = orientDb.openPool(database, user, password);
  }

  public ODatabasePool(String url, String user, String password) {
    this(url, user, password, OrientDBConfig.defaultConfig());
  }

  public ODatabasePool(String url, String user, String password, OrientDBConfig configuration) {
    OURLConnection val = OURLHelper.parseNew(url);
    orientDb = new OrientDB(val.getPath(), configuration);
    autoclose = true;
    internal = orientDb.openPool(val.getDbName(), user, password);
  }

  public ODatabasePool(String environment, String database, String user, String password) {
    this(environment, database, user, password, OrientDBConfig.defaultConfig());
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
