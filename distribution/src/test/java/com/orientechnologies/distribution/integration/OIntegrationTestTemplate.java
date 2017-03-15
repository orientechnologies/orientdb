package com.orientechnologies.distribution.integration;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.After;
import org.junit.Before;

import java.util.concurrent.TimeUnit;

/**
 * Created by frank on 15/03/2017.
 */
public abstract class OIntegrationTestTemplate {

  public static boolean firstTime = true;

  protected ODatabaseDocumentTx db;

  @Before
  public void setUp() throws Exception {

    if (firstTime) {
      System.out.println("Waiting for OrientDB to startup");
      TimeUnit.SECONDS.sleep(10);
      firstTime = false;
    }

    db = new ODatabaseDocumentTx("remote:localhost/demodb");
    db.open("admin", "admin");
  }

  @After
  public void tearDown() throws Exception {
    db.close();
  }
}
