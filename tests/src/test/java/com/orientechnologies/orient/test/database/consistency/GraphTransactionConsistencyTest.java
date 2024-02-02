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

package com.orientechnologies.orient.test.database.consistency;

import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Date;
import java.util.Iterator;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(enabled = false)
public class GraphTransactionConsistencyTest {

  private OrientDB orientDB;

  private static final int TXNUM = 10000;
  private static final int TXBATCH = 50;
  private static final int EDGENUM = 10;
  private static final int THREADS = 8;

  public void testTransactionConsistency() throws InterruptedException {
    orientDB =
        new OrientDB(
            "embedded:target/GraphTransactionConsistencyTest", OrientDBConfig.defaultConfig());

    if (!orientDB.exists("db")) {
      orientDB.execute("create database db plocal users (admin identified by 'admin' role admin)");
    }

    try (var database = orientDB.open("db", "admin", "admin")) {
      System.out.println("Checking consistency of database...");
      System.out.println(
          "Records found V=" + database.countClass("V") + " E=" + database.countClass("E"));
      final var vertices = database.query("select from V").stream().iterator();
      while (vertices.hasNext()) {
        var v = vertices.next();
        Assert.assertNotNull(v);

        final ORidBag out = v.getProperty("out_");
        if (out != null) {
          for (Iterator<OIdentifiable> it = out.rawIterator(); it.hasNext(); ) {
            final OIdentifiable edge = it.next();
            Assert.assertNotNull(edge);

            final ODocument rec = edge.getRecord();
            Assert.assertNotNull(rec);

            Assert.assertNotNull(rec.field("out"));
            Assert.assertEquals(
                ((ODocument) rec.field("out")).getIdentity(), v.getIdentity().get());

            Assert.assertNotNull(rec.field("in"));
          }
        }

        final ORidBag in = v.getProperty("in_");
        if (in != null) {
          for (Iterator<OIdentifiable> it = in.rawIterator(); it.hasNext(); ) {
            final OIdentifiable edge = it.next();
            Assert.assertNotNull(edge);

            final ODocument rec = edge.getRecord();
            Assert.assertNotNull(rec);

            Assert.assertNotNull(rec.field("in"));
            Assert.assertEquals(((ODocument) rec.field("in")).getIdentity(), v.getIdentity().get());

            Assert.assertNotNull(rec.field("out"));
          }
        }
      }

      System.out.println("Consistency ok.");

      final Thread[] threads = new Thread[THREADS];

      for (int i = 0; i < THREADS; ++i) {
        final int threadNum = i;
        threads[i] =
            new Thread(
                new Runnable() {
                  @Override
                  public void run() {
                    try (var graph = orientDB.open("db", "admin", "admin"); ) {
                      graph.begin();
                      System.out.println(
                          "THREAD " + threadNum + " Start transactions (" + TXNUM + ")");
                      for (int i = 0; i < TXNUM; ++i) {
                        var v1 = graph.newVertex();
                        v1.setProperty("i", i);
                        v1.setProperty("type", "Main");
                        v1.setProperty("lastUpdate", new Date());
                        v1.save();

                        for (int e = 0; e < EDGENUM; ++e) {
                          var v2 = graph.newVertex();
                          v2.setProperty("i", i);
                          v2.setProperty("e", e);
                          v2.setProperty("type", "Connected");
                          v2.setProperty("lastUpdate", new Date());
                          v1.addEdge(v2);

                          v1.save();
                        }

                        if (i % TXBATCH == 0) {
                          System.out.println(
                              "THREAD "
                                  + threadNum
                                  + " Committing batch of "
                                  + TXBATCH
                                  + " (i="
                                  + i
                                  + ")");
                          System.out.flush();

                          graph.commit();
                          graph.begin();

                          System.out.println(
                              "THREAD "
                                  + threadNum
                                  + " Commit ok - records found V="
                                  + graph.countClass("V")
                                  + " E="
                                  + graph.countClass("E"));
                          System.out.flush();
                        }
                      }
                    }
                  }
                });

        Thread.sleep(1000);
        threads[i].start();
      }

      for (int i = 0; i < THREADS; ++i) {
        try {
          threads[i].join();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
