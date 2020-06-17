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
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/** only for remote usage (it requires registered LiveQuery plugin) */
@Test(groups = "Query")
public class LiveQuery30TxTest extends DocumentDBBaseTest implements OCommandOutputListener {

  private final CountDownLatch latch = new CountDownLatch(2);
  private CountDownLatch unLatch = new CountDownLatch(1);

  class MyLiveQueryListener implements OLiveQueryResultListener {

    public List<OPair<String, OResult>> ops = new ArrayList<>();
    public int unsubscribe;

    @Override
    public void onCreate(ODatabaseDocument database, OResult data) {
      ops.add(new OPair<>("create", data));
      latch.countDown();
    }

    @Override
    public void onUpdate(ODatabaseDocument database, OResult before, OResult after) {
      ops.add(new OPair<>("update", after));
      latch.countDown();
    }

    @Override
    public void onDelete(ODatabaseDocument database, OResult data) {
      ops.add(new OPair<>("delete", data));
      latch.countDown();
    }

    @Override
    public void onError(ODatabaseDocument database, OException exception) {}

    @Override
    public void onEnd(ODatabaseDocument database) {
      unsubscribe = 1;
      unLatch.countDown();
    }
  }

  @Parameters(value = {"url"})
  public LiveQuery30TxTest(@Optional String url) {
    super(url);
  }

  @Test
  public void checkLiveQueryTx() throws IOException, InterruptedException {
    final String className1 = "LiveQuery30Test_checkLiveQueryTx_1";
    final String className2 = "LiveQuery30Test_checkLiveQueryTx_2";
    database.getMetadata().getSchema().createClass(className1);
    database.getMetadata().getSchema().createClass(className2);

    MyLiveQueryListener listener = new MyLiveQueryListener();

    OLiveQueryMonitor monitor = database.live("live select from " + className1, listener);
    Assert.assertNotNull(monitor);
    database.begin();
    database.command("insert into " + className1 + " set name = 'foo', surname = 'bar'");
    database.command("insert into  " + className1 + " set name = 'foo', surname = 'baz'");
    database.command("insert into " + className2 + " set name = 'foo'");
    database.commit();
    latch.await(1, TimeUnit.MINUTES);

    monitor.unSubscribe();
    database.command("insert into " + className1 + " set name = 'foo', surname = 'bax'");
    Assert.assertEquals(listener.ops.size(), 2);
    for (OPair doc : listener.ops) {
      Assert.assertEquals(doc.getKey(), "create");
      OResult res = (OResult) doc.getValue();
      Assert.assertEquals((res).getProperty("name"), "foo");
      Assert.assertNotNull(res.getProperty("@rid"));
      Assert.assertTrue(((ORID) res.getProperty("@rid")).getClusterPosition() >= 0);
    }
    unLatch.await(1, TimeUnit.MINUTES);
  }

  @Override
  @Test(enabled = false)
  public void onMessage(final String iText) {
    // System.out.print(iText);
    // System.out.flush();
  }
}
