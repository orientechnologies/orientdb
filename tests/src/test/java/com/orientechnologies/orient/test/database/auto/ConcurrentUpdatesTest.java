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
public class ConcurrentUpdatesTest {

  private final static int OPTIMISTIC_CYCLES  = 100;
  private final static int PESSIMISTIC_CYCLES = 100;
  private final static int THREADS            = 10;
  private final static int MAX_RETRIES        = 100;
  private final AtomicLong counter            = new AtomicLong();
  private final AtomicLong totalRetries       = new AtomicLong();
  protected String         url;
  private boolean          level1CacheEnabled;
  private boolean          level2CacheEnabled;
  private boolean          mvccEnabled;
  private long             startedOn;

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

              // System.out.println("Retry " + Thread.currentThread().getName() + " " + i + " - " + retry + "/" + MAX_RETRIES +
              // "...");
              Thread.sleep(retry * 10);
            } catch (ONeedRetryException e) {
              // System.out.println("Retry " + Thread.currentThread().getName() + " " + i + " - " + retry + "/" + MAX_RETRIES +
              // "...");
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

          for (int retry = 0; retry < MAX_RETRIES; ++retry) {
            try {
              db.command(new OCommandSQL(cmd)).execute();
              counter.incrementAndGet();
              break;

            } catch (OResponseProcessingException e) {
              if (e.getCause() instanceof ONeedRetryException) {
                Assert.assertTrue(e.getCause() instanceof ONeedRetryException);

                // System.out.println("SQL UPDATE - Retry " + Thread.currentThread().getName() + " " + i + " - " + retry + "/"
                // + MAX_RETRIES + "...");
                // Thread.sleep(retry * 10);
              } else {
                e.printStackTrace();
                Assert.assertTrue(false);
              }
            } catch (ONeedRetryException e) {
              // System.out.println("SQL UPDATE - Retry " + Thread.currentThread().getName() + " " + i + " - " + retry + "/"
              // + MAX_RETRIES + "...");
              // Thread.sleep(retry * 10);
            }
            // System.out.println("thread " + threadName + " counter " + counter.get());
          }
        }
      } catch (Throwable e) {
        e.printStackTrace();
        Assert.assertTrue(false);
      }
    }
  }

  @Parameters(value = "url")
  public ConcurrentUpdatesTest(@Optional(value = "memory:test") String iURL) {
    url = iURL;
  }

  @BeforeClass
  public void init() {
    level1CacheEnabled = OGlobalConfiguration.CACHE_LEVEL1_ENABLED.getValueAsBoolean();
    level2CacheEnabled = OGlobalConfiguration.CACHE_LEVEL2_ENABLED.getValueAsBoolean();
    mvccEnabled = OGlobalConfiguration.DB_MVCC.getValueAsBoolean();

    if (level1CacheEnabled)
      OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(false);
    if (level2CacheEnabled)
      OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(false);
    if (!mvccEnabled)
      OGlobalConfiguration.DB_MVCC.setValue(true);

    if ("memory:test".equals(url))
      new ODatabaseDocumentTx(url).create().close();

  }

  @AfterClass
  public void deinit() {
    OGlobalConfiguration.CACHE_LEVEL1_ENABLED.setValue(level1CacheEnabled);
    OGlobalConfiguration.CACHE_LEVEL2_ENABLED.setValue(level2CacheEnabled);
    OGlobalConfiguration.DB_MVCC.setValue(mvccEnabled);
  }

  @Test
  public void concurrentOptimisticUpdates() throws Exception {
    System.out.println("Started Test OPTIMISTIC");

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

    System.out.println("Done! Total updates executed in parallel: " + counter.get() + " average retries: "
        + ((float) totalRetries.get() / (float) counter.get()));

    Assert.assertEquals(counter.get(), OPTIMISTIC_CYCLES * THREADS);

    doc1 = databases[0].load(rid1, null, true);

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(doc1.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);

    System.out.println("RESULT doc 1:");
    System.out.println(doc1.toJSON());

    doc2 = databases[0].load(rid2, null, true);

    for (int i = 0; i < THREADS; ++i)
      Assert.assertEquals(doc2.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);

    System.out.println("RESULT doc 2:");
    System.out.println(doc2.toJSON());

    for (int i = 0; i < THREADS; ++i)
      databases[i].close();

    System.out.println("Test completed in " + (System.currentTimeMillis() - startedOn));
  }

  @Test
  public void concurrentPessimisticSQLUpdates() throws Exception {
    if (url.startsWith("local:"))
      // SKIP TEST WITH LOCAL
      return;
    sqlUpdate(true);
  }

  @Test
  public void concurrentOptimisticSQLUpdates() throws Exception {
    sqlUpdate(false);
  }

  protected void sqlUpdate(boolean lock) throws InterruptedException {
    System.out.println("Started Test " + (lock ? "LOCK" : ""));

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

    System.out.println("Done! Total sql updates executed in parallel: " + counter.get());

    Assert.assertEquals(counter.get(), PESSIMISTIC_CYCLES * THREADS);

    doc1 = databases[0].load(rid1, null, true);
    Assert.assertEquals(doc1.field("total"), PESSIMISTIC_CYCLES * THREADS);

    for (int i = 0; i < THREADS; ++i)
      databases[i].close();

    System.out.println("concurrentOptimisticSQLUpdates Test " + (lock ? "LOCK" : "") + " completed in "
        + (System.currentTimeMillis() - startedOn));
  }
}
