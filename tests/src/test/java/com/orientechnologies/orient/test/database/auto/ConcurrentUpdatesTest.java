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

import java.util.concurrent.atomic.AtomicLong;

import org.testng.Assert;
import org.testng.annotations.*;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import com.orientechnologies.orient.enterprise.channel.binary.OResponseProcessingException;

@Test
public class ConcurrentUpdatesTest {

  private final static int CYCLES       = 50;
  private final static int MAX_RETRIES  = 100;

  protected String         url;
  private boolean          level1CacheEnabled;
  private boolean          level2CacheEnabled;
  private boolean          mvccEnabled;

  private final AtomicLong counter      = new AtomicLong();
  private final AtomicLong totalRetries = new AtomicLong();

  class UpdateField implements Runnable {

    ODatabaseDocumentTx db;
    ORID                rid1;
    ORID                rid2;
    String              fieldValue = null;
    String              threadName;

    public UpdateField(ODatabaseDocumentTx iDb, ORID iRid1, ORID iRid2, String iThreadName) {
      super();
      db = iDb;
      rid1 = iRid1;
      rid2 = iRid2;
      threadName = iThreadName;
    }

    public void run() {
      try {
        for (int i = 0; i < CYCLES; i++) {
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

              System.out.println("Retry " + Thread.currentThread().getName() + " " + i + " - " + retry + "/" + MAX_RETRIES + "...");
              Thread.sleep(retry * 10);
            } catch (ONeedRetryException e) {
              System.out.println("Retry " + Thread.currentThread().getName() + " " + i + " - " + retry + "/" + MAX_RETRIES + "...");
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
  public void concurrentUpdates() throws Exception {
    ODatabaseDocumentTx database1 = new ODatabaseDocumentTx(url).open("admin", "admin");
    ODatabaseDocumentTx database2 = new ODatabaseDocumentTx(url).open("admin", "admin");
    ODatabaseDocumentTx database3 = new ODatabaseDocumentTx(url).open("admin", "admin");

    ODocument doc1 = database1.newInstance();
    doc1.field("INIT", "ok");
    database1.save(doc1);
    ORID rid1 = doc1.getIdentity();

    ODocument doc2 = database1.newInstance();
    doc2.field("INIT", "ok");
    database1.save(doc2);
    ORID rid2 = doc2.getIdentity();

    UpdateField vUpdate1 = new UpdateField(database1, rid1, rid2, "thread1");
    UpdateField vUpdate2 = new UpdateField(database2, rid2, rid1, "thread2");
    UpdateField vUpdate3 = new UpdateField(database3, rid2, rid1, "thread3");

    Thread vThread1 = new Thread(vUpdate1, "ConcurrentTest1");
    Thread vThread2 = new Thread(vUpdate2, "ConcurrentTest2");
    Thread vThread3 = new Thread(vUpdate3, "ConcurrentTest3");

    vThread1.start();
    vThread2.start();
    vThread3.start();

    vThread1.join();
    vThread2.join();
    vThread3.join();

    System.out.println("Done! Total updates executed in parallel: " + counter.get() + " average retries: "
        + ((float) totalRetries.get() / (float) counter.get()));

    Assert.assertEquals(counter.get(), CYCLES * 3);

    doc1 = database1.load(rid1, null, true);
    Assert.assertEquals(doc1.field(vUpdate1.threadName), vUpdate1.fieldValue, vUpdate1.threadName);
    Assert.assertEquals(doc1.field(vUpdate2.threadName), vUpdate2.fieldValue, vUpdate2.threadName);
    Assert.assertEquals(doc1.field(vUpdate3.threadName), vUpdate3.fieldValue, vUpdate3.threadName);
    System.out.println("RESULT doc 1:");
    System.out.println(doc1.toJSON());

    doc2 = database1.load(rid2, null, true);
    Assert.assertEquals(doc2.field(vUpdate1.threadName), vUpdate1.fieldValue, vUpdate1.threadName);
    Assert.assertEquals(doc2.field(vUpdate2.threadName), vUpdate2.fieldValue, vUpdate2.threadName);
    Assert.assertEquals(doc2.field(vUpdate3.threadName), vUpdate3.fieldValue, vUpdate3.threadName);
    System.out.println("RESULT doc 2:");
    System.out.println(doc2.toJSON());

    database1.close();
    database2.close();
  }
}
