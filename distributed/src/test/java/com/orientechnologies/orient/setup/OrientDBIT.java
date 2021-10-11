package com.orientechnologies.orient.setup;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.ODatabaseException;

// Adds retries to the operations provided by OrientDB
public class OrientDBIT extends OrientDB {
  private static final int MAX_OPEN_RETRY = 5;
  private static final int OPEN_RETRY_INTERVAL_SECONDS = 15;
  private static final int MAX_DROP_RETRY = 5;
  private static final int DROP_RETRY_INTERVAL_SECONDS = 5;

  public OrientDBIT(
      String url, String serverUser, String serverPassword, OrientDBConfig configuration) {
    super(url, serverUser, serverPassword, configuration);
  }

  @Override
  public ODatabaseSession open(String database, String user, String password) {
    // todo: this needs a timeout, otherwise can get stuck sometime!
    int i = 1;
    while (true) {
      try {
        TestSetupUtil.log("Trying (%d/%d) to open database '%s'.", i, MAX_OPEN_RETRY, database);
        return super.open(database, user, password);
      } catch (ODatabaseException e) {
        if (i++ >= MAX_OPEN_RETRY) {
          throw e;
        }
        try {
          Thread.sleep(OPEN_RETRY_INTERVAL_SECONDS * 1000);
        } catch (InterruptedException interruptedException) {
        }
      }
    }
  }

  @Override
  public void create(String database, ODatabaseType type) {
    TestSetupUtil.log("Creating database '%s'.", database);
    super.create(database, type);
  }
}
