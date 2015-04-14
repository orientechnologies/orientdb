package com.orientechnologies.orient.core.db;

import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;

public class ODatabaseCreateDropClusterTest {

  @Test
  public void createDropCluster() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODatabaseCreateDropClusterTest.class.getName());
    db.create();
    try {
      db.addCluster("test");
      Assert.assertNotEquals(db.getClusterIdByName("test"), -1);
      db.dropCluster("test", false);
      Assert.assertEquals(db.getClusterIdByName("test"), -1);
    } finally {
      db.drop();
    }
  }

  @Test
  public void createDropClusterOnClass() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + ODatabaseCreateDropClusterTest.class.getName());
    db.create();
    try {
      OClass test = db.getMetadata().getSchema().createClass("test");
      test.addCluster("aTest");
      Assert.assertNotEquals(db.getClusterIdByName("aTest"), -1);
      Assert.assertEquals(test.getClusterIds().length, 2);
      db.dropCluster("aTest", false);
      Assert.assertEquals(db.getClusterIdByName("aTest"), -1);
      test = db.getMetadata().getSchema().getClass("test");
      Assert.assertEquals(test.getClusterIds().length, 1);
    } finally {
      db.drop();
    }
  }

}
