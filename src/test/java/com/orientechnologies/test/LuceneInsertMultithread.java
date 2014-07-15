/*
 *
 *  * Copyright 2014 Orient Technologies.
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *  
 */

package com.orientechnologies.test;

import com.orientechnologies.lucene.collections.OFullTextCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.tx.OTransaction;
import org.testng.Assert;
import org.testng.annotations.Test;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Created by enricorisa on 28/06/14.
 */

public class LuceneInsertMultithread {

  private final static int  THREADS = 10;
  private final static int  CYCLE   = 1000;
  private ODatabaseDocument databaseDocumentTx;
  private static String     url;
  static {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    if (buildDirectory == null)
      buildDirectory = ".";

    // url = "plocal:" + buildDirectory + "/multiThread";
    url = "remote:localhost/multiThread";
  }

  @Test(enabled = false)
  public static class LuceneInsertThread implements Runnable {

    private ODatabaseDocumentTx db;
    private int                 cycle     = 0;
    private int                 commitBuf = 500;

    public LuceneInsertThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      db = new ODatabaseDocumentTx(url);
      db.open("admin", "admin");
      db.declareIntent(new OIntentMassiveInsert());
      db.begin(OTransaction.TXTYPE.NOTX);
      for (int i = 0; i < cycle; i++) {
        ODocument doc = new ODocument("City");

        doc.field("name", "Sono andato a scuola e quindi boh.");

        db.save(doc);
        if (i % commitBuf == 0) {
          db.commit();
          db.begin();
        }

      }

      db.close();
    }
  }

  public class LuceneReadThread implements Runnable {
    private final int         cycle;
    private ODatabaseDocument databaseDocumentTx;

    public LuceneReadThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      databaseDocumentTx = new ODatabaseDocumentTx(url);
      databaseDocumentTx.open("admin", "admin");
      OSchema schema = databaseDocumentTx.getMetadata().getSchema();
      OIndex idx = schema.getClass("City").getClassIndex("City.name");
      List<String> keys = new ArrayList<String>();
      keys.add("andato");

      for (int i = 0; i < cycle; i++) {
        Collection<?> coll = (Collection<?>) idx.get(new OFullTextCompositeKey(keys));
        System.out.println(coll.size());
      }

    }
  }

  public LuceneInsertMultithread() {
    super();
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {

    databaseDocumentTx = new ODatabaseDocumentTx(url);
    if (!url.contains("remote:") && databaseDocumentTx.exists()) {
      databaseDocumentTx.open("admin", "admin");
      databaseDocumentTx.drop();
      databaseDocumentTx.create();
    } else {
      databaseDocumentTx.open("admin", "admin");
    }

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    if (schema.getClass("City") == null) {
      OClass oClass = schema.createClass("City");

      oClass.createProperty("name", OType.STRING);
      oClass.createIndex("City.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });
    }

    LuceneInsertThread[] opts = new LuceneInsertThread[THREADS];

    for (int i = 0; i < THREADS; ++i)
      opts[i] = new LuceneInsertThread(CYCLE);

    Thread read = new Thread(new LuceneReadThread(CYCLE));
    Thread[] threads = new Thread[THREADS + 1];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(opts[i], "ConcurrentTest" + i);

    threads[THREADS] = read;
    for (int i = 0; i < THREADS + 1; ++i)
      threads[i].start();

    for (int i = 0; i < THREADS + 1; ++i)
      threads[i].join();

    OIndex idx = schema.getClass("City").getClassIndex("City.name");

    Assert.assertEquals(idx.getSize(), THREADS * CYCLE);
  }
}
