/*
 *
 *  *  Copyright 2015 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.OCluster;
import org.junit.Assert; import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by luigidellaquila on 13/04/15.
 */

public class OLiveQueryTest {

  private CountDownLatch latch = new CountDownLatch(2);

  class MyLiveQueryListener implements OLiveResultListener {

    public List<ORecordOperation> ops = new ArrayList<ORecordOperation>();

    @Override
    public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
      ops.add(iOp);
      latch.countDown();
    }

    @Override
    public void onError(int iLiveToken) {

    }

    @Override
    public void onUnsubscribe(int iLiveToken) {

    }
  }

  @Test
  public void testLiveInsert() throws InterruptedException {

    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.create();
    try {
      db.getMetadata().getSchema().createClass("test");
      db.getMetadata().getSchema().createClass("test2");
      MyLiveQueryListener listener = new MyLiveQueryListener();

      OLegacyResultSet<ODocument> tokens = db.query(new OLiveQuery<ODocument>("live select from test", listener));
      Assert.assertEquals(tokens.size(), 1);

      ODocument tokenDoc = tokens.get(0);
      Integer token = tokenDoc.field("token");
      Assert.assertNotNull(token);

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar'")).execute();
      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'baz'")).execute();
      db.command(new OCommandSQL("insert into test2 set name = 'foo'")).execute();

      latch.await(1, TimeUnit.MINUTES);

      db.command(new OCommandSQL("live unsubscribe " + token)).execute();

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bax'")).execute();
      db.command(new OCommandSQL("insert into test2 set name = 'foo'"));
      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'baz'")).execute();

      Assert.assertEquals(listener.ops.size(), 2);
      for (ORecordOperation doc : listener.ops) {
        Assert.assertEquals(doc.type, ORecordOperation.CREATED);
        Assert.assertEquals(((ODocument) doc.record).field("name"), "foo");
      }
    } finally {

      db.drop();
    }
  }

  @Test
  public void testLiveInsertOnCluster() {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.create();
    try {
      OClass clazz = db.getMetadata().getSchema().createClass("test");

      int defaultCluster = clazz.getDefaultClusterId();
      OCluster cluster = db.getStorage().getClusterById(defaultCluster);

      MyLiveQueryListener listener = new MyLiveQueryListener();

      db.query(new OLiveQuery<ODocument>("live select from cluster:" + cluster.getName(), listener));

      db.command(new OCommandSQL("insert into cluster:" + cluster.getName() + " set name = 'foo', surname = 'bar'")).execute();

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

  @Test
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.create();
    try {
      OSchema schema = db.getMetadata().getSchema();
      OClass oRestricted = schema.getClass("ORestricted");
      schema.createClass("test", oRestricted);

      int liveMatch = 1;
      List<ODocument> query = db.query(new OSQLSynchQuery("select from OUSer where name = 'reader'"));

      final OIdentifiable reader = query.iterator().next().getIdentity();
      final OIdentifiable current = db.getUser().getIdentity();

      ExecutorService executorService = Executors.newSingleThreadExecutor();

      final CountDownLatch latch = new CountDownLatch(1);
      Future<Integer> future = executorService.submit(new Callable<Integer>() {
        @Override
        public Integer call() throws Exception {
          ODatabaseDocumentTx db = new ODatabaseDocumentTx("memory:OLiveQueryTest");
          db.open("reader", "reader");

          final AtomicInteger integer = new AtomicInteger(0);
          db.query(new OLiveQuery<ODocument>("live select from test", new OLiveResultListener() {
            @Override
            public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
              integer.incrementAndGet();
            }

            @Override
            public void onError(int iLiveToken) {

            }

            @Override
            public void onUnsubscribe(int iLiveToken) {

            }
          }));

          latch.countDown();
          Thread.sleep(3000);
          return integer.get();
        }
      });

      latch.await();

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar'")).execute();

      db.command(new OCommandSQL("insert into test set name = 'foo', surname = 'bar', _allow=?"))
          .execute(new ArrayList<OIdentifiable>() {{
            add(current);
            add(reader);
          }});

      Integer integer = future.get();
      Assert.assertEquals(integer.intValue(), liveMatch);
    } finally {
      db.drop();
    }
  }

}
