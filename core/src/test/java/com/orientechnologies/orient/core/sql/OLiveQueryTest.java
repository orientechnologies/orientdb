package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.storage.OCluster;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 13/04/15.
 */
@Test public class OLiveQueryTest {

  static class MyLiveQueryListener implements OLiveResultListener {

    public List<ORecordOperation> ops = new ArrayList<ORecordOperation>();

    @Override public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
      ops.add(iOp);
    }
  }

  @Test public void testLiveInsert() {
    OLiveCommandExecutorSQLFactory.init();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.create();
    db.registerHook(new OLiveQueryHook(db));
    try {
      db.getMetadata().getSchema().createClass("test");
      db.getMetadata().getSchema().createClass("test2");
      MyLiveQueryListener listener = new MyLiveQueryListener();

      db.query(new OLiveQuery<ODocument>("live select from test", listener));

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar'")).execute();
      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'baz'")).execute();
      db.command(new OCommandSQL("insert into test2 set name = 'foo'"));

      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(listener.ops.size(), 2);
      for (ORecordOperation doc : listener.ops) {
        Assert.assertEquals(doc.type, ORecordOperation.CREATED);
        Assert.assertEquals(((ODocument) doc.record).field("name"), "foo");
      }
    } finally {

      db.drop();
    }
  }

  @Test(enabled = false)
  public void testLiveInsertOnCluster() {
    OLiveCommandExecutorSQLFactory.init();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.create();
    db.registerHook(new OLiveQueryHook(db));
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("test");

      int defaultCluster = clazz.getDefaultClusterId();
      OCluster cluster = db.getStorage().getClusterById(defaultCluster);

      MyLiveQueryListener listener = new MyLiveQueryListener();

      db.query(new OLiveQuery<ODocument>("live select from cluster:" + cluster.getName(), listener));

      db.command(new OCommandSQL("insert into cluster:" + cluster.getName() + " set name = 'foo', surname = 'bar'"))
          .execute();

      try {
        Thread.sleep(3000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(listener.ops.size(), 1);
      for (ORecordOperation doc : listener.ops) {
        Assert.assertEquals(doc.type, ORecordOperation.CREATED);
        Assert.assertEquals(((ODocument) doc.record).field("name"), "foo");
      }
    } finally {
      db.drop();
    }
  }

}
