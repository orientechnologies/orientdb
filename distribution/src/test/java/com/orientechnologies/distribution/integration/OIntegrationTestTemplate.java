package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.After;
import org.junit.Before;

/**
 * This abstract class is a template to be extended to implements integration tests.
 *
 * <p>
 *
 * <p>Created by frank on 15/03/2017.
 */
public abstract class OIntegrationTestTemplate extends OSingleOrientDBServerBaseIT {

  protected ODatabaseDocument db;

  @Before
  public void setUp() throws Exception {

    String serverUrl =
        "remote:" + container.getContainerIpAddress() + ":" + container.getMappedPort(2424);

    orientDB = new OrientDB(serverUrl, "root", "root", OrientDBConfig.defaultConfig());

    pool = new ODatabasePool(orientDB, "demodb", "admin", "admin");

    db = pool.acquire();
  }

  @After
  public void tearDown() {
    db.activateOnCurrentThread();
    db.close();
    pool.close();
    orientDB.close();
  }
}
