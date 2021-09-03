package com.orientechnologies.orient.core.db.document;

import com.orientechnologies.orient.core.exception.OSchemaException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by tglman on 04/11/15. */
public class ODatabaseDocumentSaveClusterTest {

  private ODatabaseDocument db;

  @Before
  public void before() {
    db =
        new ODatabaseDocumentTx("memory:" + ODatabaseDocumentSaveClusterTest.class.getSimpleName());
    db.create();
  }

  @Test(expected = IllegalArgumentException.class)
  public void testSaveWrongCluster() {
    db.getMetadata().getSchema().createClass("test");
    db.addCluster("test_one");

    db.save(new ODocument("test"), "test_one");
  }

  @Test(expected = OSchemaException.class)
  public void testUsedClusterTest() {
    OClass clazz = db.getMetadata().getSchema().createClass("test");
    db.addCluster("test_one");
    clazz.addCluster("test_one");
    OClass clazz2 = db.getMetadata().getSchema().createClass("test2");
    clazz2.addCluster("test_one");
  }

  @Test
  public void testSaveCluster() {
    OClass clazz = db.getMetadata().getSchema().createClass("test");
    int res = db.addCluster("test_one");
    clazz.addCluster("test_one");

    ORecord saved = db.save(new ODocument("test"), "test_one");
    Assert.assertEquals(saved.getIdentity().getClusterId(), res);
  }

  @Test
  public void testDeleteClassAndClusters() {
    OClass clazz = db.getMetadata().getSchema().createClass("test");
    int res = db.addCluster("test_one");
    clazz.addCluster("test_one");

    ORecord saved = db.save(new ODocument("test"), "test_one");
    Assert.assertEquals(saved.getIdentity().getClusterId(), res);
    db.getMetadata().getSchema().dropClass(clazz.getName());
    Assert.assertFalse(db.existsCluster("test_one"));
  }

  @After
  public void after() {
    db.drop();
  }
}
