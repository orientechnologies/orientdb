package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OView;
import com.orientechnologies.orient.core.metadata.schema.OViewConfig;
import com.orientechnologies.orient.core.record.OElement;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * @author Luigi Dell'Aquila (l.dellaquila-(at)-orientdb.com)
 */
public class OCreateViewStatementExecutionTest {
  static ODatabaseDocument db;

  @BeforeClass
  public static void beforeClass() {
    db = new ODatabaseDocumentTx("memory:OCreateViewStatementExecutionTest");
    db.create();
  }

  @AfterClass
  public static void afterClass() {
    db.close();
  }

  @Test
  public void testPlain() {
    String className = "testPlain";
    OResultSet result = db.command("create view " + className + "  FROM (SELECT FROM V)");
    OSchema schema = db.getMetadata().getSchema();
    OView view = schema.getView(className);
    Assert.assertNotNull(view);
    Assert.assertEquals(className, view.getName());
    result.close();
  }

  @Test
  public void testOriginField() throws InterruptedException {
    String className = "testOriginFieldClass";
    String viewName = "testOriginFieldView";
    db.createClass(className);

    OElement elem = db.newElement(className);
    elem.setProperty("name", "foo");
    elem.save();

    OViewConfig cfg = new OViewConfig(viewName, "SELECT FROM " + className);
    cfg.setOriginRidField("origin");
    CountDownLatch latch = new CountDownLatch(1);
    db.getMetadata().getSchema().createView(cfg, new ViewCreationListener() {
      @Override
      public void afterCreate(String viewName) {
        latch.countDown();
      }

      @Override
      public void onError(String viewName, Exception exception) {
        latch.countDown();
      }
    });

    latch.await();

    OResultSet rs = db.query("SELECT FROM " + viewName);
    Assert.assertTrue(rs.hasNext());
    OResult item = rs.next();
    Assert.assertEquals(elem.getIdentity(), item.getProperty("origin"));
    Assert.assertEquals("foo", item.getProperty("name"));
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

}
