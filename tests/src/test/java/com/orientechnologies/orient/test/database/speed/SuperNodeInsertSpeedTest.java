package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.common.test.SpeedTestMultiThreads;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.intent.OIntentMassiveInsert;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientGraphNoTx;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Test;

/** @author Artem Orobets (enisher-at-gmail.com) */
/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language
 * governing permissions and limitations under the License.
 */

@Test(enabled = false)
public class SuperNodeInsertSpeedTest extends OrientMultiThreadTest {
  protected static final String URL = "plocal:target/databases/graphspeedtest";
  protected final OrientGraphFactory factory = new OrientGraphFactory(URL);
  protected static final AtomicLong counter = new AtomicLong();
  protected static ORID superNodeRID;

  @Test(enabled = false)
  public static class CreateObjectsThread extends OrientThreadTest {
    protected OrientBaseGraph graph;
    protected OrientVertex superNode;

    public CreateObjectsThread(final SpeedTestMultiThreads parent, final int threadId) {
      super(parent, threadId);
    }

    @Override
    public void init() {
      OrientGraphFactory factory = new OrientGraphFactory(URL);
      graph = factory.getNoTx();
      factory.close();

      graph.getRawGraph().declareIntent(new OIntentMassiveInsert());

      superNode = graph.getVertex(superNodeRID);
    }

    public void cycle() {
      final OrientVertex v =
          graph.addVertex(
              "class:Client,cluster:client_" + currentThreadId(), "uid", counter.getAndIncrement());

      superNode.addEdge("test", v);
    }

    @Override
    public void deinit() throws Exception {
      if (graph != null) graph.shutdown();
      super.deinit();
    }

    private int currentThreadId() {
      return threadId;
    }
  }

  public SuperNodeInsertSpeedTest() {
    super(100000, 16, CreateObjectsThread.class);
  }

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    // System.setProperty("url", "memory:test");
    OGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    SuperNodeInsertSpeedTest test = new SuperNodeInsertSpeedTest();
    test.data.go(test);
  }

  @Override
  public void init() {
    if (factory.exists()) factory.drop();

    final OrientGraphNoTx graph = factory.getNoTx();

    try {
      if (graph.getVertexType("Client") == null) {
        final OrientVertexType clientType = graph.createVertexType("Client");

        final OrientVertexType.OrientVertexProperty property =
            clientType.createProperty("uid", OType.STRING);
        // property.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);

        // CREATE ONE CLUSTER PER THREAD
        for (int i = 0; i < getThreads(); ++i) {
          System.out.println("Creating cluster: client_" + i + "...");
          clientType.addCluster("client_" + i);
        }
      }

      OrientVertex superNode = graph.addVertex("class:Client", "name", "superNode");
      final OrientVertex v = graph.addVertex("class:Client", "uid", counter.getAndIncrement());
      superNode.addEdge("test", v);

      superNodeRID = superNode.getIdentity();

    } finally {
      graph.shutdown();
    }
  }

  @Override
  public void deinit() {
    final OrientGraphNoTx graph = factory.getNoTx();
    try {
      final long total = graph.countVertices("Client");

      System.out.println("\nTotal objects in Client cluster after the test: " + total);
      System.out.println("Created " + (total));
      Assert.assertEquals(total, threadCycles);

      //      final long indexedItems =
      // graph.getRawGraph().getMetadata().getIndexManager().getIndex("Client.uid").getSize();
      //      System.out.println("\nTotal indexed objects after the test: " + indexedItems);

    } finally {
      graph.shutdown();
    }
  }
}
