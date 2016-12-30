/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */

package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * @author Sergey Sitnikov
 */
public class FreezeAndRecordInsertAtomicityTest {

  private static final String URL;
  private static final int THREADS    = Runtime.getRuntime().availableProcessors() * 2;
  private static final int ITERATIONS = 50;

  static {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null)
      buildDirectory = "./target";

    URL = "plocal:" + buildDirectory + File.separator + FreezeAndRecordInsertAtomicityTest.class.getSimpleName();
  }

  private Random              random;
  private ODatabaseDocumentTx db;
  private ExecutorService     executorService;
  private CountDownLatch      countDownLatch;

  @Before
  public void before() {
    final long seed = System.currentTimeMillis();
    System.out.println(FreezeAndRecordInsertAtomicityTest.class.getSimpleName() + " seed: " + seed);
    random = new Random(seed);

    db = new ODatabaseDocumentTx(URL);
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
    db.getMetadata().getSchema().createClass("Person").createProperty("name", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);

    executorService = Executors.newFixedThreadPool(THREADS);

    countDownLatch = new CountDownLatch(THREADS);
  }

  @After
  public void after() throws InterruptedException {
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

    db.drop();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    final Set<Future<?>> futures = new HashSet<Future<?>>();

    for (int i = 0; i < THREADS; ++i) {
      final int thread = i;

      futures.add(executorService.submit(new Runnable() {
        @Override
        public void run() {
          try {
            final ODatabaseDocumentTx db = new ODatabaseDocumentTx(URL);
            db.open("admin", "admin");
            final OIndex<?> index = db.getMetadata().getIndexManager().getIndex("Person.name");

            for (int i = 0; i < ITERATIONS; ++i)
              switch (random.nextInt(3)) {
              case 0:
                db.newInstance("Person").field("name", "name-" + thread + "-" + i).save();
                break;

              case 1:
                db.begin();
                db.newInstance("Person").field("name", "name-" + thread + "-" + i).save();
                db.commit();
                break;

              case 2:
                db.freeze();
                try {
                  for (ODocument document : db.browseClass("Person"))
                    assertEquals(document.getIdentity(), index.get(document.field("name")));
                } finally {
                  db.release();
                }

                break;
              }
          } finally {
            countDownLatch.countDown();
          }
        }
      }));
    }

    countDownLatch.await();

    for (Future<?> future : futures)
      future.get(); // propagate exceptions, if there are any
  }

}
