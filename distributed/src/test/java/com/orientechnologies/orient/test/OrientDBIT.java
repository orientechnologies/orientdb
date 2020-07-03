package com.orientechnologies.orient.test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OStorageException;

// Adds retries to the operations provided by OrientDB
public class OrientDBIT extends OrientDB {
  public OrientDBIT(String url, String serverUser, String serverPassword, OrientDBConfig configuration) {
    super(url, serverUser, serverPassword, configuration);
  }

  @Override public ODatabaseSession open(String database, String user, String password) {
    // todo: this needs a timeout, otherwise can get stuck sometime!
    int i = 1, max = 10;
    while (true) {
      try {
        System.out.printf("Trying (%d/%d) to open database %s.\n", i, max, database);
        return super.open(database, user, password);
      } catch (ODatabaseException e) {
        if (i++ >= max) {
          throw e;
        }
        try {
          Thread.sleep(15000);
        } catch (InterruptedException interruptedException) {
        }
      }
    }
  }

  @Override public void drop(String database) {
    int i = 1, max = 5;
    while (true) {
      try {
        System.out.printf("Trying (%d/%d) to drop database %s.\n", i, max, database);
        super.drop(database);
        break;
      } catch (OStorageException e) {
        if (i++ >= max) {
          throw e;
        }
        try {
          Thread.sleep(10000);
        } catch (InterruptedException interruptedException) {
        }
      }
    }
  }
}
