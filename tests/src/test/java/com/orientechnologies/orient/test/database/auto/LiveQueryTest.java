/*
 * Copyright 2010-2012 Luca Garulli (l.garulli--at--orientechnologies.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * only for remote usage (it requires registered LiveQuery plugin)
 */
@Test(groups = "Query")
public class LiveQueryTest extends DocumentDBBaseTest implements OCommandOutputListener {

  private final CountDownLatch latch = new CountDownLatch(2);

  class MyLiveQueryListener implements OLiveResultListener {

    public List<ORecordOperation> ops = new ArrayList<ORecordOperation>();

    @Override
    public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
      ops.add(iOp);
      latch.countDown();
    }

    @Override public void onError(int iLiveToken) {

    }

    @Override public void onUnsubscribe(int iLiveToken) {

    }
  }

  @Parameters(value = { "url" })
  public LiveQueryTest(@Optional String url) {
    super(url);
  }

  @Test(enabled = false)
  public void checkLiveQuery1() throws IOException {
    final String className1 = "LiveQueryTest1_1";
    final String className2 = "LiveQueryTest1_2";
    database.getMetadata().getSchema().createClass(className1);
    database.getMetadata().getSchema().createClass(className2);

    MyLiveQueryListener listener = new MyLiveQueryListener();

    OResultSet<ODocument> tokens = database.query(new OLiveQuery<ODocument>("live select from " + className1, listener));
    Assert.assertEquals(tokens.size(), 1);
    ODocument tokenDoc = tokens.get(0);
    Integer token = tokenDoc.field("token");
    Assert.assertNotNull(token);

    database.command(new OCommandSQL("insert into " + className1 + " set name = 'foo', surname = 'bar'")).execute();
    database.command(new OCommandSQL("insert into  " + className1 + " set name = 'foo', surname = 'baz'")).execute();
    database.command(new OCommandSQL("insert into " + className2 + " set name = 'foo'"));
    try {
      latch.await();
    } catch (InterruptedException e) {
      e.printStackTrace();
    }

    database.command(new OCommandSQL("live unsubscribe " + token)).execute();
    database.command(new OCommandSQL("insert into " + className1 + " set name = 'foo', surname = 'bax'")).execute();

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
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    // System.out.print(iText);
    // System.out.flush();
  }
}
