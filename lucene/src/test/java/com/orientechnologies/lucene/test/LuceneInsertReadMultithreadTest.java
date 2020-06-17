/*
 *
 *  * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by enricorisa on 28/06/14. */
public class LuceneInsertReadMultithreadTest extends BaseLuceneTest {

  private static final int THREADS = 10;
  private static final int RTHREADS = 1;
  private static final int CYCLE = 100;

  protected String url = "";

  @Before
  public void init() {

    url = db.getURL();
    OSchema schema = db.getMetadata().getSchema();
    OClass oClass = schema.createClass("City");

    oClass.createProperty("name", OType.STRING);
    db.command(new OCommandSQL("create index City.name on City (name) FULLTEXT ENGINE LUCENE"))
        .execute();
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {

    db.getMetadata().reload();
    OSchema schema = db.getMetadata().getSchema();

    Thread[] threads = new Thread[THREADS + RTHREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(new LuceneInsertThread(CYCLE), "ConcurrentWriteTest" + i);

    for (int i = THREADS; i < THREADS + RTHREADS; ++i)
      threads[i] = new Thread(new LuceneReadThread(CYCLE), "ConcurrentReadTest" + i);

    for (int i = 0; i < THREADS + RTHREADS; ++i) threads[i].start();

    System.out.println(
        "Started LuceneInsertReadMultithreadBaseTest test, waiting for "
            + threads.length
            + " threads to complete...");

    for (int i = 0; i < THREADS + RTHREADS; ++i) threads[i].join();

    System.out.println("LuceneInsertReadMultithreadBaseTest all threads completed");

    OIndex idx = schema.getClass("City").getClassIndex("City.name");

    Assert.assertEquals(idx.getInternal().size(), THREADS * CYCLE);
  }

  public class LuceneInsertThread implements Runnable {

    private ODatabaseSession db;
    private int cycle = 0;
    private int commitBuf = 500;

    public LuceneInsertThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      db = openDatabase();

      db.declareIntent(new OIntentMassiveInsert());
      db.begin();
      for (int i = 0; i < cycle; i++) {
        ODocument doc = new ODocument("City");

        doc.field("name", "Rome");

        db.save(doc);
        if (i % commitBuf == 0) {
          db.commit();
          db.begin();
        }
      }
      db.commit();

      db.close();
    }
  }

  public class LuceneReadThread implements Runnable {
    private final int cycle;
    private ODatabaseSession databaseDocumentTx;

    public LuceneReadThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      databaseDocumentTx = openDatabase();

      OSchema schema = databaseDocumentTx.getMetadata().getSchema();
      OIndex idx = schema.getClass("City").getClassIndex("City.name");

      for (int i = 0; i < cycle; i++) {

        databaseDocumentTx
            .command(new OSQLSynchQuery<ODocument>("select from city where name LUCENE 'Rome'"))
            .execute();
      }
    }
  }
}
