package com.orientechnologies.orient.core.db;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseCreateDropClusterTest extends BaseMemoryDatabase {

  @Test
  public void createDropCluster() {
    db.addCluster("test");
    Assert.assertNotEquals(db.getClusterIdByName("test"), -1);
    db.dropCluster("test");
    Assert.assertEquals(db.getClusterIdByName("test"), -1);
  }

  @Test
  public void createDropClusterOnClass() {
    OClass test = db.getMetadata().getSchema().createClass("test", 1, null);
    test.addCluster("aTest");
    Assert.assertNotEquals(db.getClusterIdByName("aTest"), -1);
    Assert.assertEquals(test.getClusterIds().length, 2);
    db.dropCluster("aTest");
    Assert.assertEquals(db.getClusterIdByName("aTest"), -1);
    test = db.getMetadata().getSchema().getClass("test");
    Assert.assertEquals(test.getClusterIds().length, 1);
  }
}
