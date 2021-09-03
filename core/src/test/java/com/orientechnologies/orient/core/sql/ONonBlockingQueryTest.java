package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;
import org.junit.Test;

/** Created by luigidellaquila on 13/04/15. */
public class ONonBlockingQueryTest {

  static class MyResultListener implements OCommandResultListener {

    private CountDownLatch latch;
    public int numResults = 0;
    public boolean finished = false;

    MyResultListener(CountDownLatch latch) {
      this.latch = latch;
    }

    @Override
    public boolean result(Object iRecord) {
      latch.countDown();
      numResults++;
      return true;
    }

    @Override
    public void end() {
      finished = true;
      latch.countDown();
    }

    @Override
    public Object getResult() {
      return null;
    }
  }

  @Test
  public void testExceptionManagement() {
    // issue #5244
    OLiveCommandExecutorSQLFactory.init();

    ODatabaseDocument db = new ODatabaseDocumentTx("memory:ONonBlockingQueryTest");
    db.activateOnCurrentThread();
    db.create();

    db.getMetadata().getSchema().createClass("test");
    MyResultListener listener = new MyResultListener(new CountDownLatch(1));
    try {
      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar'")).execute();

      db.query(new OSQLNonBlockingQuery<Object>("select from test bla blu", listener));
      try {
        listener.latch.await(1, TimeUnit.MINUTES);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(listener.finished, true);

      listener = new MyResultListener(new CountDownLatch(2));
      db.query(new OSQLNonBlockingQuery<Object>("select from test", listener));

    } finally {
      db.close();
    }
    try {
      assertTrue(listener.latch.await(1, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(listener.numResults, 1);
  }
}
