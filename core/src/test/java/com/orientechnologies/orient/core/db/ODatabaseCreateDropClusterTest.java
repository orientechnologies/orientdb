package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseCreateDropClusterTest {

  @Test
  public void createDropCluster() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + ODatabaseCreateDropClusterTest.class.getSimpleName());
    db.create();
    try {
      db.addCluster("test");
      Assert.assertNotEquals(db.getClusterIdByName("test"), -1);
      db.dropCluster("test");
      Assert.assertEquals(db.getClusterIdByName("test"), -1);
    } finally {
      db.drop();
    }
  }

  @Test
  public void createDropClusterOnClass() {
    ODatabaseDocument db =
        new ODatabaseDocumentTx("memory:" + ODatabaseCreateDropClusterTest.class.getSimpleName());
    db.create();
    try {
      OClass test = db.getMetadata().getSchema().createClass("test", 1, null);
      test.addCluster("aTest");
      Assert.assertNotEquals(db.getClusterIdByName("aTest"), -1);
      Assert.assertEquals(test.getClusterIds().length, 2);
      db.dropCluster("aTest");
      Assert.assertEquals(db.getClusterIdByName("aTest"), -1);
      test = db.getMetadata().getSchema().getClass("test");
      Assert.assertEquals(test.getClusterIds().length, 1);
    } finally {
      db.drop();
    }
  }
}
