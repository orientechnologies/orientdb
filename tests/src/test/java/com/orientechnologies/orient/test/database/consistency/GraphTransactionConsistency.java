/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
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
 *  * For more information: http://www.orientechnologies.com
 *  
 */
package com.orientechnologies.orient.test.database.consistency;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.Date;
import java.util.Iterator;

@Test
public class GraphTransactionConsistency {
  private OrientBaseGraph  database;
  private boolean          txMode  = true;
  private final static int TXNUM   = 10000;
  private final static int TXBATCH = 50;
  private final static int EDGENUM = 10;
  private final static int THREADS = 8;

  public void testTransactionConsistency() throws InterruptedException {
    final OrientGraphFactory factory = new OrientGraphFactory("plocal:target/GraphTransactionConsistency");

    database = txMode ? factory.getTx() : factory.getNoTx();

    System.out.println("Checking consistency of database...");
    System.out.println("Records found V=" + database.countVertices() + " E=" + database.countEdges());

    final Iterable<OrientVertex> vertices = database.command(new OCommandSQL("select from V")).execute();
    for (OrientVertex v : vertices) {
      final ODocument doc = v.getRecord();

      Assert.assertNotNull(doc);

      final ORidBag out = doc.field("out_");
      if (out != null) {
        for (Iterator<OIdentifiable> it = out.rawIterator(); it.hasNext();) {
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
        for (Iterator<OIdentifiable> it = in.rawIterator(); it.hasNext();) {
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
      threads[i] = new Thread(new Runnable() {
        @Override
        public void run() {
          OrientBaseGraph graph = txMode ? factory.getTx() : factory.getNoTx();
          try {
            System.out.println("THREAD " + threadNum + " Start transactions (" + TXNUM + ")");
            for (int i = 0; i < TXNUM; ++i) {
              final OrientVertex v1 = graph.addVertex(null, "v", i, "type", "Main", "lastUpdate", new Date());

              for (int e = 0; e < EDGENUM; ++e) {
                final OrientVertex v2 = graph.addVertex(null, "v", i, "e", e, "type", "Connected", "lastUpdate", new Date());
                v1.addEdge("E", v2);
              }

              if (i % TXBATCH == 0) {
                System.out.println("THREAD " + threadNum + " Committing batch of " + TXBATCH + " (i=" + i + ")");
                System.out.flush();

                graph.commit();

                System.out.println(
                    "THREAD " + threadNum + " Commit ok - records found V=" + graph.countVertices() + " E=" + graph.countEdges());
                System.out.flush();
              }
            }
          } finally {
            graph.shutdown();
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

    database.shutdown();
  }
}
