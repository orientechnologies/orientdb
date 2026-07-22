package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import org.junit.After;
import org.junit.Before;

public abstract class OSingleOrientDBServerWithDatabasePerTestMethodBaseIT
    extends OSingleOrientDBServerBaseIT {

  @Before
  public void setupOrientDBAndPool() throws Exception {

    String dbName = name.getMethodName();

    String serverUrl =
        "remote:" + container.getContainerIpAddress() + ":" + container.getMappedPort(2424);

    orientDB = new OrientDB(serverUrl, "root", "root", OrientDBConfig.defaultConfig());

    if (orientDB.exists(dbName)) orientDB.drop(dbName);
    orientDB.createIfNotExists(dbName, ODatabaseType.PLOCAL);

    pool = new ODatabasePool(orientDB, dbName, "admin", "admin");
  }

  @After
  public void tearDown() {
    pool.close();
    orientDB.close();
  }
}
