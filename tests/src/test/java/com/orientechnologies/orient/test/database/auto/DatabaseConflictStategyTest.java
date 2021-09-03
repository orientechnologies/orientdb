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

package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public final class DatabaseConflictStategyTest {

  private String dbName;
  private OrientGraphFactory graphReadFactory;

  public DatabaseConflictStategyTest(String dbName) {
    this.dbName = dbName;
  }

  public static void main(String args[]) {
    DatabaseConflictStategyTest test =
        new DatabaseConflictStategyTest("DatabaseConflictStategyTest");
    test.runTest();
    Runtime.getRuntime().halt(0);
  }

  public void runTest() {
    OrientBaseGraph orientGraph = new OrientGraphNoTx(getDBURL());
    log("Set database CONFLICTSTRATEGY to automerge");
    orientGraph.command(new OCommandSQL("ALTER database CONFLICTSTRATEGY 'automerge'")).execute();
    createVertexType(orientGraph, "Test");
    orientGraph.shutdown();

    OrientBaseGraph graph = getGraphFactory().getTx();

    Vertex vertex = graph.addVertex("class:Test");
    vertex.setProperty("prop1", "v1-1");
    vertex.setProperty("prop2", "v2-1");
    vertex.setProperty("prop3", "v3-1");
    graph.shutdown();

    Thread th1 = startThread(2, 1000, "prop1");
    Thread th2 = startThread(3, 2000, "prop1");
    Thread th3 = startThread(4, 3000, "prop1");
    try {
      th1.join();
      th2.join();
      th3.join();
    } catch (Exception ex) {
      ex.printStackTrace();
    }
  }

  public void printVertex(String info, OrientVertex vtx) {
    //    System.out.println("--------" + info + " ----------");
    //    System.out.println(vtx);
    //    Set<String> keys = vtx.getPropertyKeys();
    //    for (String key : keys) {
    //      System.out.println("Key = " + key + " Value = " + vtx.getProperty(key));
    //    }
  }

  /** @return */
  public String getDBURL() {
    return "memory:" + dbName;
  }

  private Thread startThread(final int version, final long timeout, final String key) {

    Thread th =
        new Thread() {
          @Override
          public void run() {
            OrientVertex vtx1 = null;
            OrientGraph graph = getGraphFactory().getTx();
            Iterable<Vertex> vtxs = graph.getVertices();
            for (Vertex vtx : vtxs) {
              vtx1 = (OrientVertex) vtx;
            }
            try {
              Thread.sleep(timeout);
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            vtx1.setProperty(key, "key-" + version);
            graph.commit();
            printVertex(version + "", vtx1);
            graph.shutdown();
          }
        };
    th.start();
    return th;
  }

  private OrientGraphFactory getGraphFactory() {
    if (graphReadFactory == null) {
      log("Datastore pool created with size : 10, db location: " + getDBURL());
      graphReadFactory = new OrientGraphFactory(getDBURL()).setupPool(1, 10);
    }
    return graphReadFactory;
  }

  private void createVertexType(OrientBaseGraph orientGraph, String className) {
    OClass clazz = orientGraph.getVertexType(className);
    if (clazz == null) {
      log("Creating vertex type - " + className);
      orientGraph.createVertexType(className);
    }
  }

  private void log(String message) {
    //    System.out.println(message);
  }

  private void log(String message, Throwable th) {
    System.out.println(th.getMessage());
    th.printStackTrace();
  }
}
