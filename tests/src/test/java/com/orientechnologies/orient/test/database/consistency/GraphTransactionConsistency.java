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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test
public class GraphTransactionConsistency {
  private ODatabaseDocument database;
  private boolean txMode = true;
  private static final int TXNUM = 10000;
  private static final int TXBATCH = 50;
  private static final int EDGENUM = 10;
  private static final int THREADS = 8;

  public void testTransactionConsistency() throws InterruptedException {
    OrientDB context = new OrientDB("embedded:target/", OrientDBConfig.defaultConfig());
    context.execute(
        "create database GraphTransactionConsistency plocal users(admin identified by 'adminpwd'"
            + " role admin)");

    database = context.open("GraphTransactionConsistency", "admin", "adminpwd");

    System.out.println("Checking consistency of database...");
    System.out.println(
        "Records found V=" + database.countClass("V") + " E=" + database.countClass("E"));

    final List<OVertex> vertices =
        database.query("select from V").stream()
            .map((r) -> r.getVertex().get())
            .collect(Collectors.toList());
    for (OVertex v : vertices) {
      final ODocument doc = v.getRecord();

      Assert.assertNotNull(doc);

      final ORidBag out = doc.field("out_");
      if (out != null) {
        for (Iterator<OIdentifiable> it = out.rawIterator(); it.hasNext(); ) {
          final OIdentifiable edge = it.next();
          Assert.assertNotNull(edge);

          final ODocument rec = edge.getRecord();
          Assert.assertNotNull(rec);

          Assert.assertNotNull(rec.field("out"));
          Assert.assertEquals(((ODocument) rec.field("out")).getIdentity(), v.getIdentity());

          Assert.assertNotNull(rec.field("in"));
        }
      }

      final ORidBag in = doc.field("in_");
      if (in != null) {
        for (Iterator<OIdentifiable> it = in.rawIterator(); it.hasNext(); ) {
          final OIdentifiable edge = it.next();
          Assert.assertNotNull(edge);

          final ODocument rec = edge.getRecord();
          Assert.assertNotNull(rec);

          Assert.assertNotNull(rec.field("in"));
          Assert.assertEquals(((ODocument) rec.field("in")).getIdentity(), v.getIdentity());

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
                  ODatabaseDocument graph =
                      context.open("GraphTransactionConsistency", "admin", "adminpwd");
                  try {
                    System.out.println(
                        "THREAD " + threadNum + " Start transactions (" + TXNUM + ")");
                    for (int i = 0; i < TXNUM; ++i) {
                      final OVertex v1 = graph.newVertex();
                      v1.setProperty("v", i);
                      v1.setProperty("type", "Main");
                      v1.setProperty("lastUpdate", new Date());
                      graph.save(v1);

                      for (int e = 0; e < EDGENUM; ++e) {
                        final OVertex v2 = graph.newVertex();
                        v2.setProperty("v", i);
                        v2.setProperty("e", e);
                        v2.setProperty("type", "Connected");
                        v2.setProperty("lastUpdate", new Date());
                        graph.save(v2);

                        OEdge edge = graph.newEdge(v1, v2, "E");
                        graph.save(edge);
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
                  } finally {
                    graph.close();
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

    database.close();
    context.close();
  }
}
