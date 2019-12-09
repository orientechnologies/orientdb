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
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 * Created by enricorisa on 28/06/14.
 */

public class LuceneInsertMultithreadTest {

  private final static int           THREADS  = 10;
  private final static int           RTHREADS = 1;
  private final static int           CYCLE    = 100;
  private static       String        buildDirectory;
  private static final String        dbName;
  private static final ODatabaseType databaseType;
  private static final OrientDB      orientDB;

  static {
    System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) {
      buildDirectory = ".";
    }

    String config = System.getProperty("orientdb.test.env");

    String storageType;
    if ("ci".equals(config) || "release".equals(config)) {
      storageType = OEngineLocalPaginated.NAME;
      databaseType = ODatabaseType.PLOCAL;
    } else {
      storageType = OEngineMemory.NAME;
      databaseType = ODatabaseType.MEMORY;
    }

    dbName = "multiThread";
    orientDB = new OrientDB(storageType + ":" + buildDirectory, OrientDBConfig.defaultConfig());
  }

  public LuceneInsertMultithreadTest() {
    super();
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {
    if (orientDB.exists(dbName)) {
      orientDB.drop(dbName);
    }

    orientDB.create(dbName, databaseType);
    ODatabaseDocument databaseDocumentTx = orientDB.open(dbName, "admin", "admin");

    OSchema schema = databaseDocumentTx.getMetadata().getSchema();
    if (schema.getClass("City") == null) {
      OClass oClass = schema.createClass("City");

      oClass.createProperty("name", OType.STRING);
      oClass.createIndex("City.name", "FULLTEXT", null, null, "LUCENE", new String[] { "name" });
    }

    Thread[] threads = new Thread[THREADS + RTHREADS];
    for (int i = 0; i < THREADS; ++i)
      threads[i] = new Thread(new LuceneInsertThread(CYCLE), "ConcurrentWriteTest" + i);

    for (int i = THREADS; i < THREADS + RTHREADS; ++i)
      threads[i] = new Thread(new LuceneReadThread(CYCLE), "ConcurrentReadTest" + i);

    for (int i = 0; i < THREADS + RTHREADS; ++i)
      threads[i].start();

    for (int i = 0; i < THREADS + RTHREADS; ++i)
      threads[i].join();

    OIndex idx = schema.getClass("City").getClassIndex("City.name");

    Assertions.assertThat(idx.getInternal().size()).isEqualTo(THREADS * CYCLE);
    orientDB.drop(dbName);
  }

  public static class LuceneInsertThread implements Runnable {

    private       ODatabaseSession db;
    private final int              cycle;
    private final int              commitBuf = 500;

    LuceneInsertThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      db = orientDB.open(dbName, "admin", "admin");
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
    private final int               cycle;
    private       ODatabaseDocument databaseDocumentTx;

    LuceneReadThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {
      databaseDocumentTx = orientDB.open(dbName, "admin", "admin");
      OSchema schema = databaseDocumentTx.getMetadata().getSchema();
      OIndex idx = schema.getClass("City").getClassIndex("City.name");

      for (int i = 0; i < cycle; i++) {
        idx.get("Rome");
      }

    }
  }
}
