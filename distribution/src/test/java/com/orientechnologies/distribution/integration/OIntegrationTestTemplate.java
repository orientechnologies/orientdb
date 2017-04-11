package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
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

  protected ODatabaseDocument        db;
  private   OPartitionedDatabasePool pool;

  @Before
  public void setUp() throws Exception {

    if (firstTime) {
      System.out.println("Waiting for OrientDB to startup");
      TimeUnit.SECONDS.sleep(10);
      firstTime = false;
    }

    pool = new OPartitionedDatabasePool("remote:localhost/demodb", "admin", "admin");

    db = pool.acquire();
  }

  @After
  public void tearDown() throws Exception {
    db.activateOnCurrentThread();
    db.close();
    pool.close();
  }
}
