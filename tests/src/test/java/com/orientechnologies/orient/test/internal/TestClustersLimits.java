package com.orientechnologies.orient.test.internal;

import java.io.IOException;

import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.storage.OStorage.CLUSTER_TYPE;

@Test
public class TestClustersLimits {
  @Test
  public void testMemory() throws IOException {
    executeTest("memory:hugeclusterdb", CLUSTER_TYPE.MEMORY);
  }

  @Test
  public void testLocal() throws IOException {
    executeTest("local:C:/temp/hugeclusterdb", CLUSTER_TYPE.PHYSICAL);
  }

  @Test
  public void testRemote() throws IOException {
    executeTest("remote:localhost/hugeclusterdb", CLUSTER_TYPE.PHYSICAL);
  }

  protected static void executeTest(String url, CLUSTER_TYPE clusterType) throws IOException {
    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);

    if (ODatabaseHelper.existsDatabase(database))
      ODatabaseHelper.dropDatabase(database);

    ODatabaseHelper.createDatabase(database, url);

    database.open("admin", "admin");

    for (int i = database.getClusters(); i < Short.MAX_VALUE; ++i) {
      System.out.println("Creating cluster: " + i);
      database.addCluster("cluster" + i, clusterType);
      new ODocument().field("id", i).save("cluster" + i);
    }
    database.close();

    database.open("admin", "admin");
    System.out.println("Total clusters: " + database.getClusters());
    database.close();
  }
}