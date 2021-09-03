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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ConcurrentUpdatesTest extends DocumentDBBaseTest {

  private static final int OPTIMISTIC_CYCLES = 100;
  private static final int PESSIMISTIC_CYCLES = 100;
  private static final int THREADS = 10;

  private final AtomicLong counter = new AtomicLong();
  private final AtomicLong totalRetries = new AtomicLong();
  private boolean mvccEnabled;

  @Parameters(value = "url")
  public ConcurrentUpdatesTest(@Optional String url) {
    super(url);
  }

  class OptimisticUpdateField implements Runnable {

    ORID rid1;
    ORID rid2;
    String fieldValue = null;
    String threadName;
    String url;

    public OptimisticUpdateField(String url, ORID iRid1, ORID iRid2, String iThreadName) {
      super();
      rid1 = iRid1;
      rid2 = iRid2;
      threadName = iThreadName;
      this.url = url;
    }

    @Override
    public void run() {
      try {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
        for (int i = 0; i < OPTIMISTIC_CYCLES; i++) {
          int retries = 0;
          while (true) {
            retries++;
            if (retries % 10 == 0)
              System.out.println(retries + " retries for thread " + threadName);

            try {

              db.open("admin", "admin");
              try {
                db.begin(TXTYPE.OPTIMISTIC);

                ODocument vDoc1 = db.load(rid1, null, true);
                vDoc1.field(threadName, vDoc1.field(threadName) + ";" + i);
                vDoc1.save();

                ODocument vDoc2 = db.load(rid2, null, true);
                vDoc2.field(threadName, vDoc2.field(threadName) + ";" + i);
                vDoc2.save();

                db.commit();
              } finally {
                db.close();
              }

              counter.incrementAndGet();
              totalRetries.addAndGet(retries);
              break;
            } catch (ONeedRetryException e) {
              Thread.sleep(retries * 10);
            }
          }
          fieldValue += ";" + i;
        }

      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  class PessimisticUpdate implements Runnable {
    String fieldValue = null;
    ORID rid;
    String threadName;
    boolean lock;
    String url;

    public PessimisticUpdate(String url, ORID iRid, String iThreadName, boolean iLock) {
      super();

      rid = iRid;
      threadName = iThreadName;
      lock = iLock;
      this.url = url;
    }

    @Override
    public void run() {
      try {
        ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);

        for (int i = 0; i < PESSIMISTIC_CYCLES; i++) {
          String cmd = "update " + rid + " increment total = 1";
          if (lock) cmd += " lock record";

          int retries = 0;
          while (true) {
            try {
              retries++;

              db.open("admin", "admin");
              try {
                db.command(new OCommandSQL(cmd)).execute();
                counter.incrementAndGet();
              } finally {
                db.close();
              }

              if (retries % 10 == 0)
                System.out.println(retries + " retries for thread " + threadName);

              break;

            } catch (ONeedRetryException e) {
              if (lock) {
                Assert.fail(ONeedRetryException.class.getSimpleName() + " was encountered");
              }
            }
          }
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  @Test
  public void concurrentOptimisticUpdates() throws Exception {
    counter.set(0);

    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODocument doc1 = database.newInstance();
    doc1.field("INIT", "ok");
    database.save(doc1, database.getClusterNameById(database.getDefaultClusterId()));
    ORID rid1 = doc1.getIdentity();

    ODocument doc2 = database.newInstance();
    doc2.field("INIT", "ok");
    database.save(doc2, database.getClusterNameById(database.getDefaultClusterId()));
    ORID rid2 = doc2.getIdentity();

    OptimisticUpdateField[] ops = new OptimisticUpdateField[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new OptimisticUpdateField(url, rid1, rid2, "thread" + i);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i) threads[i] = new Thread(ops[i], "ConcurrentTest" + i);

    for (int i = 0; i < THREADS; ++i) threads[i].start();

    for (int i = 0; i < THREADS; ++i) threads[i].join();

    Assert.assertEquals(counter.get(), OPTIMISTIC_CYCLES * THREADS);

    doc1 = database.load(rid1, null, true);

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(doc1.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);

    doc1.toJSON();

    doc2 = database.load(rid2, null, true);

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(doc2.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);

    doc2.toJSON();
    System.out.println(doc2.toJSON());

    database.close();
  }

  @Test(enabled = false)
  public void concurrentPessimisticSQLUpdates() throws Exception {
    sqlUpdate(true);
  }

  @Test
  public void concurrentOptimisticSQLUpdates() throws Exception {
    sqlUpdate(false);
  }

  protected void sqlUpdate(boolean lock) throws InterruptedException {
    counter.set(0);

    ODatabaseDocumentTx database = new ODatabaseDocumentTx(url);
    database.open("admin", "admin");

    ODocument doc1 = database.newInstance();
    doc1.field("total", 0);
    database.save(doc1, database.getClusterNameById(database.getDefaultClusterId()));
    ORID rid1 = doc1.getIdentity();

    PessimisticUpdate[] ops = new PessimisticUpdate[THREADS];
    for (int i = 0; i < THREADS; ++i) ops[i] = new PessimisticUpdate(url, rid1, "thread" + i, lock);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i) threads[i] = new Thread(ops[i], "ConcurrentTest" + i);

    for (int i = 0; i < THREADS; ++i) threads[i].start();

    for (int i = 0; i < THREADS; ++i) threads[i].join();

    Assert.assertEquals(counter.get(), PESSIMISTIC_CYCLES * THREADS);

    doc1 = database.load(rid1, null, true);
    Assert.assertEquals(doc1.<Object>field("total"), PESSIMISTIC_CYCLES * THREADS);

    database.close();
  }
}
