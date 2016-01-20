package com.orientechnologies.orient.core.metadata.schema;

import static org.testng.AssertJUnit.assertEquals;

import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;

public class AlterRemoveClusterTest {

  @Test
  public void testRemoveClusterDefaultCluster() {
    ODatabaseDocument db = new ODatabaseDocumentTx("memory:" + AlterRemoveClusterTest.class.getSimpleName());
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("Test");
      clazz.addCluster("TestOneMore");

      clazz.removeClusterId(db.getClusterIdByName("Test"));
      db.getMetadata().getSchema().reload();
      clazz = db.getMetadata().getSchema().getClass("Test");
      assertEquals(clazz.getDefaultClusterId(), db.getClusterIdByName("TestOneMore"));

      clazz.removeClusterId(db.getClusterIdByName("TestOneMore"));
      assertEquals(clazz.getDefaultClusterId(), -1);

    } finally {
      db.drop();
    }

  }
}
