package com.orientechnologies.orient.core.sql;

import com.orientechnologies.orient.core.command.OCommandResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.sql.query.OSQLNonBlockingQuery;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 * Created by luigidellaquila on 13/04/15.
 */
@Test
public class ONonBlockingQueryTest {

  static class MyResultListener implements OCommandResultListener {

    public int     numResults = 0;
    public boolean finished   = false;

    @Override public boolean result(Object iRecord) {
      numResults++;
      return true;
    }

    @Override public void end() {
      finished = true;
    }
  }

  @Test
  public void testExceptionManagement() {
    //issue #5244
    OLiveCommandExecutorSQLFactory.init();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:ONonBlockingQueryTest");
    db.activateOnCurrentThread();
    db.create();

    db.getMetadata().getSchema().createClass("test");
    MyResultListener listener = new MyResultListener();
    try {
      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar'")).execute();

      db.query(new OSQLNonBlockingQuery<Object>("select from test bla blu", listener));
      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(listener.finished, true);

      listener = new MyResultListener();
      db.query(new OSQLNonBlockingQuery<Object>("select from test", listener));

    } finally {
      db.close();
    }
    try {
      Thread.sleep(3000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(listener.numResults, 1);

  }

}
