package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

/**
 * This abstract class is a template to be extended to implements integration tests.
 * <p>
 * <p>
 * Created by frank on 15/03/2017.
 */
public abstract class OIntegrationTestTemplate {

  public static boolean firstTime = true;

  protected ODatabaseDocument db;
  protected OrientDB          orientDB;
  private   ODatabasePool     pool;

  @Before
  public void setUp() throws Exception {

    if (firstTime) {
      System.out.println("Waiting for OrientDB to startup");
      TimeUnit.SECONDS.sleep(5);
      firstTime = false;
    }

    //root's user password is defined inside the pom
    orientDB = new OrientDB("remote:localhost", "root", "root", OrientDBConfig.defaultConfig());

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
