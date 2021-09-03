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
package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.tx.OTransaction.TXTYPE;
import java.util.ArrayList;

public class OrientDbWriteLotsOfDataTest {

  private static final String DBURI = "remote:localhost/demo";
  private static final String DBUSR = "admin";
  private static final String DBPWD = "admin";
  private static final String CLASSNAME = "WriteLotsTestClass";
  private static final String PROPKEY = "key";
  private static final String PROPVAL = "value";

  public static void main(String[] args) throws InterruptedException {
    OrientDbWriteLotsOfDataTest test = new OrientDbWriteLotsOfDataTest();
    // loop: for each loop, a set of threads writes to the database
    // each thread writes 10000 odocs to the database with 1000 odocs/txn
    for (int k = 0; k < 50; k++) {
      test.testThreaded(5, TXTYPE.NOTX);
      System.out.printf("finished run %d:\n", k);
    }
  }

  public void testThreaded(int numthreads, TXTYPE txtype) throws InterruptedException {

    // create document pool
    OPartitionedDatabasePool pool = new OPartitionedDatabasePool(DBURI, DBUSR, DBPWD);

    // create the schema for the test class if it doesn't exist
    ODatabaseDocumentTx db = pool.acquire();
    try {
      OSchema schema = db.getMetadata().getSchema();
      if (!schema.existsClass(CLASSNAME)) {
        OClass oc = schema.createClass(CLASSNAME);
        oc.createProperty(PROPKEY, OType.STRING);
        oc.setStrictMode(true);
      }
    } finally {
      db.close();
    }

    // create threads and execute

    // create threads, put into list
    ArrayList<RunTest> threads = new ArrayList<RunTest>(1000);
    for (int numth = 0; numth < numthreads; numth++) {
      threads.add(new RunTest(pool, txtype));
    }
    // run test: start each thread and wait for all threads to complete with join
    long a = System.currentTimeMillis();
    for (RunTest t : threads) {
      t.start();
    }
    for (RunTest t : threads) {
      t.join();
    }
    long b = System.currentTimeMillis();

    // collect from each thread
    int savesum = 0;
    int txnsum = 0;
    for (RunTest t : threads) {
      t.printStats();
      savesum += t.getTotalSaves();
      txnsum += t.getTotalTxns();
    }

    // print out cummulative stats
    double secs = 1.0E-3D * (b - a);
    System.out.printf(
        "TOTAL: [%4.2f secs %d tx, %d save] %.2f tx/sec %.2f save/sec \n",
        secs, txnsum, savesum, txnsum / secs, savesum / secs);
  }

  class RunTest extends Thread {

    private ODatabaseDocumentTx db;
    private int statTotalTxn;
    private int statTotalSaves;
    private double statTotalSecs;
    private TXTYPE txtype;

    public RunTest(OPartitionedDatabasePool pool, TXTYPE txtype) {
      this.txtype = txtype;
      this.db = pool.acquire();
    }

    public int getTotalTxns() {
      return this.statTotalTxn;
    }

    public int getTotalSaves() {
      return this.statTotalSaves;
    }

    public double getTotalSecs() {
      return this.statTotalSecs;
    }

    public void printStats() {
      long thid = this.getId();
      int ntx = this.getTotalTxns();
      int nsave = this.getTotalSaves();
      double secs = this.getTotalSecs();
      System.out.printf(
          "Thd%2d: [%4.2f secs %d tx, %d save] %.2f tx/sec %.2f save/sec \n",
          thid, secs, ntx, nsave, ntx / secs, nsave / secs);
    }

    @Override
    public void run() {
      try {
        int savecount = 0;
        int txncount = 0;
        long t0, t1;
        switch (this.txtype) {
          case NOTX:
            {
              t0 = System.currentTimeMillis();
              final ODocument foo = new ODocument();
              db.begin(TXTYPE.NOTX);
              int ktxn;
              for (ktxn = 0; ktxn < 10; ktxn++) {
                int ksave;
                for (ksave = 0; ksave < 1000; ++ksave) {
                  foo.reset();
                  foo.setClassName(CLASSNAME);
                  foo.field(PROPKEY, PROPVAL);
                  db.save(foo);
                }
                savecount += ksave;
              }
              db.commit();
              txncount += ktxn;
              t1 = System.currentTimeMillis();
              break;
            }
          case OPTIMISTIC:
            {
              t0 = System.currentTimeMillis();
              db.begin(TXTYPE.OPTIMISTIC);
              int ktxn;
              for (ktxn = 0; ktxn < 10; ktxn++) {
                int ksave;
                for (ksave = 0; ksave < 1000; ++ksave) {
                  ODocument foo = new ODocument(CLASSNAME);
                  foo.field(PROPKEY, PROPVAL);
                  db.save(foo);
                }
                savecount += ksave;
                // this.totalcounter.addAndGet(ksave);
              }
              db.commit();
              txncount += ktxn;
              t1 = System.currentTimeMillis();
              break;
            }
          default:
            throw new Error("unknown txtype: " + txtype);
        }
        this.statTotalSaves = savecount;
        this.statTotalTxn = txncount;
        this.statTotalSecs = 1.0E-3 * (t1 - t0);
      } catch (Exception e) {
        db.rollback();
        // throw e;
      } finally {
        db.close();
      }
    }
  }
}
