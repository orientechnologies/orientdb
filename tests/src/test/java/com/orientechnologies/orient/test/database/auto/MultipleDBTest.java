/**
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

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.OClusterPositionFactory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTx;

/**
 * @author Michael Hiess
 */
public class MultipleDBTest {

  private String baseUrl;

  @Parameters(value = "url")
  public MultipleDBTest(String iURL) {
    baseUrl = iURL + "-";
  }

  @Test
  public void testObjectMultipleDBsThreaded() throws Exception {
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    Set<Future> threads = new HashSet<Future>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    for (int i = 0; i < dbs; i++) {

      final String dbUrl = baseUrl + i;

      Callable<Void> t = new Callable<Void>() {

        public Void call() throws InterruptedException, IOException {
          OObjectDatabaseTx tx = new OObjectDatabaseTx(dbUrl);

          ODatabaseHelper.deleteDatabase(tx, "plocal");
          ODatabaseHelper.createDatabase(tx, dbUrl, "plocal");

          try {
            System.out.println("(" + getDbId(tx) + ") " + "Created");

            if (tx.isClosed()) {
              tx.open("admin", "admin");
            }
            tx.getEntityManager().registerEntityClass(DummyObject.class);

            long start = System.currentTimeMillis();
            for (int j = 0; j < operations_write; j++) {
              DummyObject dummy = new DummyObject("name" + j);

              Assert.assertEquals(ODatabaseRecordThreadLocal.INSTANCE.get().getURL(), dbUrl);

              dummy = tx.save(dummy);

              if (!dbUrl.startsWith("plocal:"))
                // CAN'T WORK FOR LHPEPS CLUSTERS BECAUSE CLUSTER POSITION CANNOT BE KNOWN
                Assert.assertEquals(((ORID) dummy.getId()).getClusterPosition(), OClusterPositionFactory.INSTANCE.valueOf(j),
                    "RID was " + dummy.getId());

              if ((j + 1) % 20000 == 0) {
                System.out.println("(" + getDbId(tx) + ") " + "Operations (WRITE) executed: " + (j + 1));
              }
            }
            long end = System.currentTimeMillis();

            String time = "(" + getDbId(tx) + ") " + "Executed operations (WRITE) in: " + (end - start) + " ms";
            System.out.println(time);
            times.add(time);

            start = System.currentTimeMillis();
            for (int j = 0; j < operations_read; j++) {
              List<DummyObject> l = tx.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
              Assert.assertEquals(l.size(), operations_write);

              if ((j + 1) % 20000 == 0) {
                System.out.println("(" + getDbId(tx) + ") " + "Operations (READ) executed: " + j + 1);
              }
            }
            end = System.currentTimeMillis();

            time = "(" + getDbId(tx) + ") " + "Executed operations (READ) in: " + (end - start) + " ms";
            System.out.println(time);
            times.add(time);

            tx.close();

          } finally {
            System.out.println("(" + getDbId(tx) + ") " + "Dropping");
            System.out.flush();
            ODatabaseHelper.deleteDatabase(tx, "plocal");
            System.out.println("(" + getDbId(tx) + ") " + "Dropped");
            System.out.flush();
          }
          return null;
        }
      };

      threads.add(executorService.submit(t));
    }

    for (Future future : threads)
      future.get();

    System.out.println("Test testObjectMultipleDBsThreaded ended");
  }

  @Test
  public void testDocumentMultipleDBsThreaded() throws Exception {

    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    Set<Future> results = new HashSet<Future>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    for (int i = 0; i < dbs; i++) {

      final String dbUrl = baseUrl + i;

      Callable<Void> t = new Callable<Void>() {

        public Void call() throws InterruptedException, IOException {
          ODatabaseDocumentTx tx = new ODatabaseDocumentTx(dbUrl);

          ODatabaseHelper.deleteDatabase(tx, "plocal");
          System.out.println("Thread " + this + " is creating database " + dbUrl);
          System.out.flush();
          ODatabaseHelper.createDatabase(tx, dbUrl, "plocal");

          try {
            System.out.println("(" + getDbId(tx) + ") " + "Created");
            System.out.flush();

            if (tx.isClosed()) {
              tx.open("admin", "admin");
            }

            long start = System.currentTimeMillis();
            for (int j = 0; j < operations_write; j++) {

              ODocument dummy = new ODocument("DummyObject");
              dummy.field("name", "name" + j);

              Assert.assertEquals(ODatabaseRecordThreadLocal.INSTANCE.get().getURL(), dbUrl);

              dummy = tx.save(dummy);

              if (!dbUrl.startsWith("plocal:"))
                // CAN'T WORK FOR LHPEPS CLUSTERS BECAUSE CLUSTER POSITION CANNOT BE KNOWN
                Assert.assertEquals(dummy.getIdentity().getClusterPosition(), OClusterPositionFactory.INSTANCE.valueOf(j),
                    "RID was " + dummy.getIdentity());

              if ((j + 1) % 20000 == 0) {
                System.out.println("(" + getDbId(tx) + ") " + "Operations (WRITE) executed: " + (j + 1));
                System.out.flush();
              }
            }
            long end = System.currentTimeMillis();

            String time = "(" + getDbId(tx) + ") " + "Executed operations (WRITE) in: " + (end - start) + " ms";
            System.out.println(time);
            System.out.flush();

            times.add(time);

            start = System.currentTimeMillis();
            for (int j = 0; j < operations_read; j++) {
              List<DummyObject> l = tx.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
              Assert.assertEquals(l.size(), operations_write);

              if ((j + 1) % 20000 == 0) {
                System.out.println("(" + getDbId(tx) + ") " + "Operations (READ) executed: " + j + 1);
                System.out.flush();
              }
            }
            end = System.currentTimeMillis();

            time = "(" + getDbId(tx) + ") " + "Executed operations (READ) in: " + (end - start) + " ms";
            System.out.println(time);
            System.out.flush();

            times.add(time);

          } finally {
            tx.close();

            System.out.println("Thread " + this + "  is dropping database " + dbUrl);
            System.out.flush();
            ODatabaseHelper.deleteDatabase(tx, "plocal");
          }
          return null;
        }
      };

      results.add(executorService.submit(t));
    }

    for (Future future : results)
      future.get();

    System.out.println("Test testDocumentMultipleDBsThreaded ended");
    System.out.flush();
  }

  private String getDbId(ODatabase tx) {
    if (tx.getStorage() instanceof OStorageRemote)
      return tx.getURL() + " - sessionId: " + ((OStorageRemote) tx.getStorage()).getSessionId();
    else
      return tx.getURL();
  }

}
