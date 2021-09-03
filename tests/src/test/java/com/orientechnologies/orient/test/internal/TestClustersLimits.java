package com.orientechnologies.orient.test.internal;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import org.testng.annotations.Test;

@Test
public class TestClustersLimits {
  @Test
  public void testMemory() throws IOException {
    executeTest("memory:hugeclusterdb");
  }

  @Test
  public void testLocal() throws IOException {
    executeTest("plocal:C:/temp/hugeclusterdb");
  }

  @Test
  public void testRemote() throws IOException {
    executeTest("remote:localhost/hugeclusterdb");
  }

  protected static void executeTest(String url) throws IOException {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);

    if (ODatabaseHelper.existsDatabase(database, "plocal"))
      ODatabaseHelper.dropDatabase(database, "plocal");

    ODatabaseHelper.createDatabase(database, url);

    database.open("admin", "admin");

    for (int i = database.getClusters(); i < Short.MAX_VALUE; ++i) {
      System.out.println("Creating cluster: " + i);
      database.addCluster("cluster" + i);
      new ODocument().field("id", i).save("cluster" + i);
    }
    database.close();

    database.open("admin", "admin");
    System.out.println("Total clusters: " + database.getClusters());
    database.close();
  }
}
