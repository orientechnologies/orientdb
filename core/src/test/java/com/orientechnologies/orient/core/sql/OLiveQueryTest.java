/*
 *
 *  *  Copyright 2015 Orient Technologies LTD (info(at)orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.query.live.OLiveQueryHook;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by luigidellaquila on 13/04/15.
 */
@Test
public class OLiveQueryTest {

  static class MyLiveQueryListener implements OLiveResultListener {

    public List<ORecordOperation> ops = new ArrayList<ORecordOperation>();

    @Override
    public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
      ops.add(iOp);
    }

    @Override
    public void onError(int iLiveToken) {

    }

    @Override
    public void onUnsubscribe(int iLiveToken) {

    }
  }

  @Test(enabled = false)
  public void testLiveInsert() {
    OLiveCommandExecutorSQLFactory.init();

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.registerHook(new OLiveQueryHook(db));
    db.create();
    try {
      db.getMetadata().getSchema().createClass("test");
      db.getMetadata().getSchema().createClass("test2");
      MyLiveQueryListener listener = new MyLiveQueryListener();

      OResultSet<ODocument> tokens = db.query(new OLiveQuery<ODocument>("live select from test", listener));
      Assert.assertEquals(tokens.size(), 1);

      ODocument tokenDoc = tokens.get(0);
      Integer token = tokenDoc.field("token");
      Assert.assertNotNull(token);

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar'")).execute();
      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'baz'")).execute();
      db.command(new OCommandSQL("insert into test2 set name = 'foo'")).execute();

      db.command(new OCommandSQL("live unsubscribe " + token)).execute();

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bax'")).execute();
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

}
