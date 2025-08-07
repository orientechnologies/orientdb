/*
 *
 *  *  Copyright 2010-2017 OrientDB LTD (http://orientdb.com)
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/** @author Sergey Sitnikov */
public class FreezeAndRecordInsertAtomicityTest {

  private static final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
  private static final int ITERATIONS = 100;
  private Random random;
  private OrientDB ctx;
  private ODatabaseDocument db;
  private ExecutorService executorService;
  private CountDownLatch countDownLatch;

  @Before
  public void before() {
    final long seed = System.currentTimeMillis();
    System.out.println(FreezeAndRecordInsertAtomicityTest.class.getSimpleName() + " seed: " + seed);
    random = new Random(seed);
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) buildDirectory = "./target";
    ctx = new OrientDB("embedded:" + buildDirectory, OrientDBConfig.defaultConfig());
    String dbName = FreezeAndRecordInsertAtomicityTest.class.getSimpleName();
    if (ctx.exists(dbName)) {
      ctx.drop(dbName);
    }
    ctx.execute(
            "create database "
                + dbName
                + " plocal users(admin identified by 'adminpwd' role admin)")
        .close();
    db = ctx.open(dbName, "admin", "adminpwd");
    db.getMetadata()
        .getSchema()
        .createClass("Person")
        .createProperty("name", OType.STRING)
        .createIndex(OClass.INDEX_TYPE.UNIQUE);

    executorService = Executors.newFixedThreadPool(THREADS);

    countDownLatch = new CountDownLatch(THREADS);
  }

  @After
  public void after() throws InterruptedException {
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
    String dbName = FreezeAndRecordInsertAtomicityTest.class.getSimpleName();
    ctx.drop(dbName);
    ctx.close();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    final Set<Future<?>> futures = new HashSet<Future<?>>();

    for (int i = 0; i < THREADS; ++i) {
      final int thread = i;

      futures.add(
          executorService.submit(
              () -> {
                try {
                  final ODatabaseDocumentInternal db =
                      (ODatabaseDocumentInternal)
                          ctx.open(
                              FreezeAndRecordInsertAtomicityTest.class.getSimpleName(),
                              "admin",
                              "adminpwd");
                  final OIndex index =
                      db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.name");

                  for (int i1 = 0; i1 < ITERATIONS; ++i1)
                    switch (random.nextInt(3)) {
                      case 0:
                        db.save(
                            db.<ODocument>newInstance("Person")
                                .field("name", "name-" + thread + "-" + i1));
                        break;

                      case 1:
                        db.begin();
                        db.save(
                            db.<ODocument>newInstance("Person")
                                .field("name", "name-" + thread + "-" + i1));
                        db.commit();
                        break;

                      case 2:
                        db.freeze();
                        try {
                          for (ODocument document : db.browseClass("Person")) {
                            try (Stream<ORID> rids =
                                index.getInternal().getRids(document.field("name"))) {
                              assertEquals(document.getIdentity(), rids.findFirst().orElse(null));
                            }
                          }
                        } finally {
                          db.release();
                        }

                        break;
                    }
                } catch (RuntimeException | Error e) {
                  e.printStackTrace();
                  throw e;
                } finally {
                  countDownLatch.countDown();
                }
              }));
    }

    countDownLatch.await();

    for (Future<?> future : futures) future.get(); // propagate exceptions, if there are any
  }
}
