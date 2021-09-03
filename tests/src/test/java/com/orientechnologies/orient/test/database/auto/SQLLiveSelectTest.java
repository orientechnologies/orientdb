/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
import com.orientechnologies.orient.core.db.record.ORecordOperation;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import com.orientechnologies.orient.core.sql.query.OLiveQuery;
import com.orientechnologies.orient.core.sql.query.OLiveResultListener;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import org.testng.Assert;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * If some of the tests start to fail then check cluster number in queries, e.g #7:1. It can be
 * because the order of clusters could be affected due to adding or removing cluster from storage.
 */
@Test(groups = "sql-select")
@SuppressWarnings("unchecked")
public class SQLLiveSelectTest extends AbstractSelectTest {

  @Parameters(value = "url")
  public SQLLiveSelectTest(@Optional String url) throws Exception {
    super(url);
  }

  @BeforeClass
  public void init() {

    database.getMetadata().getSchema().getOrCreateClass("LiveClass");
    database.getMetadata().getSchema().getOrCreateClass("LiveClassTx");
  }

  @Test
  public void liveQueryTestTX() throws InterruptedException {

    int TOTAL_OPS = 6;
    final CountDownLatch latch = new CountDownLatch(TOTAL_OPS);
    final List<ORecordOperation> ops = Collections.synchronizedList(new ArrayList());
    OLegacyResultSet<ODocument> tokens =
        database.query(
            new OLiveQuery<Object>(
                "live select from LiveClassTx",
                new OLiveResultListener() {
                  @Override
                  public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
                    ops.add(iOp);
                    latch.countDown();
                  }

                  @Override
                  public void onError(int iLiveToken) {}

                  @Override
                  public void onUnsubscribe(int iLiveToken) {}
                }));
    Assert.assertEquals(tokens.size(), 1);

    ODocument tokenDoc = tokens.get(0);
    Integer token = tokenDoc.field("token");
    Assert.assertNotNull(token);

    database.begin();
    database
        .command(new OCommandSQL("insert into LiveClassTx set name = 'foo', surname = 'bar'"))
        .execute();
    database
        .command(new OCommandSQL("insert into LiveClassTx set name = 'foo', surname = 'baz'"))
        .execute();
    database.command(new OCommandSQL("insert into LiveClassTx set name = 'foo'")).execute();
    database.commit();

    database.begin();
    database.command(new OCommandSQL("update LiveClassTx set name = 'updated'")).execute();
    database.commit();

    latch.await();

    Assert.assertEquals(ops.size(), TOTAL_OPS);
    for (ORecordOperation doc : ops) {
      if (doc.type == ORecordOperation.CREATED) {
        Assert.assertEquals(((ODocument) doc.record).field("name"), "foo");
      } else if (doc.type == ORecordOperation.UPDATED) {
        Assert.assertEquals(((ODocument) doc.record).field("name"), "updated");
      } else {
        Assert.fail();
      }
    }
  }

  @Test
  public void liveQueryTest() throws InterruptedException {

    final CountDownLatch latch = new CountDownLatch(6);
    final List<ORecordOperation> ops = Collections.synchronizedList(new ArrayList());
    OLegacyResultSet<ODocument> tokens =
        database.query(
            new OLiveQuery<Object>(
                "live select from LiveClass",
                new OLiveResultListener() {
                  @Override
                  public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws OException {
                    ops.add(iOp);
                    latch.countDown();
                  }

                  @Override
                  public void onError(int iLiveToken) {}

                  @Override
                  public void onUnsubscribe(int iLiveToken) {}
                }));
    Assert.assertEquals(tokens.size(), 1);

    ODocument tokenDoc = tokens.get(0);
    Integer token = tokenDoc.field("token");
    Assert.assertNotNull(token);

    database
        .command(new OCommandSQL("insert into liveclass set name = 'foo', surname = 'bar'"))
        .execute();
    database
        .command(new OCommandSQL("insert into liveclass set name = 'foo', surname = 'baz'"))
        .execute();
    database.command(new OCommandSQL("insert into liveclass set name = 'foo'")).execute();

    database.command(new OCommandSQL("update liveclass set name = 'updated'")).execute();

    latch.await();

    Assert.assertEquals(ops.size(), 6);
    for (ORecordOperation doc : ops) {
      if (doc.type == ORecordOperation.CREATED) {
        Assert.assertEquals(((ODocument) doc.record).field("name"), "foo");
      } else if (doc.type == ORecordOperation.UPDATED) {
        Assert.assertEquals(((ODocument) doc.record).field("name"), "updated");
      } else {
        Assert.fail();
      }
    }
  }
}
