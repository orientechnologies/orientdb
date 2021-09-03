package com.orientechnologies.orient.core.metadata.schema;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.viewmanager.ViewCreationListener;
import java.util.concurrent.CountDownLatch;
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
  public void testSimple() throws InterruptedException {
    CountDownLatch latch = new CountDownLatch(1);
    db.getMetadata()
        .getSchema()
        .createView(
            new OViewConfig("testSimple", "SELECT FROM V"),
            new ViewCreationListener() {
              @Override
              public void afterCreate(ODatabaseSession database, String viewName) {
                latch.countDown();
              }

              @Override
              public void onError(String viewName, Exception exception) {}
            });
    latch.await();

    Assert.assertNotNull(db.getMetadata().getSchema().getView("testSimple"));
    Assert.assertNull(db.getMetadata().getSchema().getClass("testSimple"));
    Assert.assertNull(db.getMetadata().getSchema().getView("V"));
  }
}
