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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import java.util.concurrent.atomic.AtomicLong;

@Test
public class ConcurrentUpdatesTest extends DocumentDBBaseTest {

  private final static int OPTIMISTIC_CYCLES  = 100;
  private final static int PESSIMISTIC_CYCLES = 100;
  private final static int THREADS            = 10;
  private final static int MAX_RETRIES        = 100;
  private final AtomicLong counter            = new AtomicLong();
  private final AtomicLong totalRetries       = new AtomicLong();
  private boolean          level1CacheEnabled;
  private boolean          mvccEnabled;
  private long             startedOn;

  @Parameters(value = "url")
  public ConcurrentUpdatesTest(@Optional String url) {
    super(url);
  }

  class OptimisticUpdateField implements Runnable {

    ODatabaseDocumentTx db;
    ORID                rid1;
    ORID                rid2;
    String              fieldValue = null;
    String              threadName;

    public OptimisticUpdateField(ODatabaseDocumentTx iDb, ORID iRid1, ORID iRid2, String iThreadName) {
      super();
      db = iDb;
      rid1 = iRid1;
      rid2 = iRid2;
      threadName = iThreadName;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < OPTIMISTIC_CYCLES; i++) {
          for (int retry = 0; retry < MAX_RETRIES; ++retry) {
            try {
              db.begin(TXTYPE.OPTIMISTIC);

              ODocument vDoc1 = db.load(rid1, null, true);
              vDoc1.field(threadName, vDoc1.field(threadName) + ";" + i);
              vDoc1.save();

              ODocument vDoc2 = db.load(rid2, null, true);
              vDoc2.field(threadName, vDoc2.field(threadName) + ";" + i);
              vDoc2.save();

              db.commit();

              counter.incrementAndGet();
              totalRetries.addAndGet(retry);
              break;
            } catch (OResponseProcessingException e) {
              Assert.assertTrue(e.getCause() instanceof ONeedRetryException);

              Thread.sleep(retry * 10);
            } catch (ONeedRetryException e) {
              Thread.sleep(retry * 10);
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

    ODatabaseDocumentTx db;
    String              fieldValue = null;
    ORID                rid;
    String              threadName;
    boolean             lock;

    public PessimisticUpdate(ODatabaseDocumentTx iDb, ORID iRid, String iThreadName, boolean iLock) {
      super();
      db = iDb;
      rid = iRid;
      threadName = iThreadName;
      lock = iLock;
    }

    @Override
    public void run() {
      try {
        for (int i = 0; i < PESSIMISTIC_CYCLES; i++) {
          String cmd = "update " + rid + " increment total = 1";
          if (lock)
            cmd += " lock record";

          int retries = 0;
          while (true) {
            try {
              db.command(new OCommandSQL(cmd)).execute();
              if (retries > 100)
                System.out.println("Success after " + retries + " retries");

              long result = counter.incrementAndGet();
              if (result % 100 == 0 || result == PESSIMISTIC_CYCLES * THREADS) {
                System.out.println(result + " records were processed  out of " + (PESSIMISTIC_CYCLES * THREADS));
              }

              break;

            } catch (OResponseProcessingException e) {

              if (e.getCause() instanceof ONeedRetryException) {
                retries++;
              } else {
                e.printStackTrace();
                throw e;
              }
            } catch (ONeedRetryException e) {
              retries++;
            }
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  @BeforeClass
  public void init() {
    mvccEnabled = OGlobalConfiguration.DB_MVCC.getValueAsBoolean();

    if (!mvccEnabled)
      OGlobalConfiguration.DB_MVCC.setValue(true);
  }

  @AfterClass
  public void deinit() {
    OGlobalConfiguration.DB_MVCC.setValue(mvccEnabled);
  }

  @Test
  public void concurrentOptimisticUpdates() throws Exception {
    counter.set(0);
    startedOn = System.currentTimeMillis();

    ODatabaseDocumentTx[] databases = new ODatabaseDocumentTx[THREADS];
    for (int i = 0; i < THREADS; ++i)
      databases[i] = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument doc1 = databases[0].newInstance();
    doc1.field("INIT", "ok");
    databases[0].save(doc1);
    ORID rid1 = doc1.getIdentity();

    ODocument doc2 = databases[0].newInstance();
    doc2.field("INIT", "ok");
    databases[0].save(doc2);
    ORID rid2 = doc2.getIdentity();

    OptimisticUpdateField[] ops = new OptimisticUpdateField[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new OptimisticUpdateField(databases[i], rid1, rid2, "thread" + i);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(ops[i], "ConcurrentTest" + i);

    for (int i = 0; i < THREADS; ++i)
      threads[i].start();

    for (int i = 0; i < THREADS; ++i)
      threads[i].join();

    Assert.assertEquals(counter.get(), OPTIMISTIC_CYCLES * THREADS);

    doc1 = databases[0].load(rid1, null, true);

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(doc1.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);

    doc1.toJSON();

    doc2 = databases[0].load(rid2, null, true);

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(doc2.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);

    doc2.toJSON();
    System.out.println(doc2.toJSON());

    for (int i = 0; i < THREADS; ++i)
      databases[i].close();

  }

  @Test
  public void concurrentPessimisticSQLUpdates() throws Exception {
    sqlUpdate(true);
  }

  @Test
  public void concurrentOptimisticSQLUpdates() throws Exception {
    sqlUpdate(false);
  }

  protected void sqlUpdate(boolean lock) throws InterruptedException {
    counter.set(0);
    startedOn = System.currentTimeMillis();

    ODatabaseDocumentTx[] databases = new ODatabaseDocumentTx[THREADS];
    for (int i = 0; i < THREADS; ++i)
      databases[i] = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument doc1 = databases[0].newInstance();
    doc1.field("total", 0);
    databases[0].save(doc1);
    ORID rid1 = doc1.getIdentity();

    PessimisticUpdate[] ops = new PessimisticUpdate[THREADS];
    for (int i = 0; i < THREADS; ++i)
      ops[i] = new PessimisticUpdate(databases[i], rid1, "thread" + i, lock);

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(ops[i], "ConcurrentTest" + i);

    for (int i = 0; i < THREADS; ++i)
      threads[i].start();

    for (int i = 0; i < THREADS; ++i)
      threads[i].join();

    Assert.assertEquals(counter.get(), PESSIMISTIC_CYCLES * THREADS);

    doc1 = databases[0].load(rid1, null, true);
    Assert.assertEquals(doc1.field("total"), PESSIMISTIC_CYCLES * THREADS);

    for (int i = 0; i < THREADS; ++i)
      databases[i].close();
  }
}
