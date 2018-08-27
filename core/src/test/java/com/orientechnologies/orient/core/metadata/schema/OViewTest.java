package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class OViewTest {

  private ODatabaseDocumentTx db;

  @Before
  public void setUp() {
    db = new ODatabaseDocumentTx("memory:" + OViewTest.class.getSimpleName());
    db.create();
  }

  @After
  public void after() {
    db.drop();
  }

  @Test
  public void testSimple() {
    db.getMetadata().getSchema().createView("testSimple", "SELECT FROM V");
    Assert.assertNotNull(db.getMetadata().getSchema().getView("testSimple"));
    Assert.assertNull(db.getMetadata().getSchema().getClass("testSimple"));
    Assert.assertNull(db.getMetadata().getSchema().getView("V"));
  }
}
