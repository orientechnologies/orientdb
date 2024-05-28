package com.orientechnologies.orient.test.database.speed;

import com.orientechnologies.common.test.SpeedTestMultiThreads;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;
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
  protected static final OrientDB factory =
      new OrientDB("embedded:target/databases/", OrientDBConfig.defaultConfig());
  protected static final AtomicLong counter = new AtomicLong();
  protected static ORID superNodeRID;

  @Test(enabled = false)
  public static class CreateObjectsThread extends OrientThreadTest {
    protected ODatabaseSession graph;
    protected OVertex superNode;

    public CreateObjectsThread(final SpeedTestMultiThreads parent, final int threadId) {
      super(parent, threadId);
    }

    @Override
    public void init() {
      graph = factory.open("graphspeedtest", "admin", "adminpwd");

      superNode = graph.load(superNodeRID);
    }

    public void cycle() {
      final OVertex v = graph.newVertex("Client");
      v.setProperty("uid", counter.getAndIncrement());
      graph.save(v);

      graph.newEdge(superNode, v, "test");
    }

    @Override
    public void deinit() throws Exception {
      if (graph != null) graph.close();
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
    if (factory.exists("graphspeedtest")) factory.drop("graphspeedtest");

    factory.execute(
        "create database graphspeedtest plocal users(admin identified by 'adminpwd' role 'admin')");

    final ODatabaseSession graph = factory.open("graphspeedtest", "admin", "adminpwd");

    try {
      if (!graph.getMetadata().getSchema().existsClass("Client")) {
        final OClass clientType =
            graph
                .getMetadata()
                .getSchema()
                .createClass("Client", graph.getMetadata().getSchema().getClass("V"));

        final OProperty property = clientType.createProperty("uid", OType.STRING);
        // property.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);

        // CREATE ONE CLUSTER PER THREAD
        for (int i = 0; i < getThreads(); ++i) {
          System.out.println("Creating cluster: client_" + i + "...");
          clientType.addCluster("client_" + i);
        }
      }

      OVertex superNode = graph.newVertex("Client");
      superNode.setProperty("name", "superNode");
      graph.save(superNode);

      final OVertex v = graph.newVertex("Client");
      v.setProperty("uid", counter.getAndIncrement());
      graph.save(v);
      graph.save(graph.newEdge(superNode, v, "test"));

      superNodeRID = superNode.getIdentity();

    } finally {
      graph.close();
    }
  }

  @Override
  public void deinit() {
    final ODatabaseSession graph = factory.open("graphspeedtest", "admin", "adminpwd");
    try {
      final long total = graph.countClass("Client");

      System.out.println("\nTotal objects in Client cluster after the test: " + total);
      System.out.println("Created " + (total));
      Assert.assertEquals(total, threadCycles);

      //      final long indexedItems =
      // graph.getRawGraph().getMetadata().getIndexManager().getIndex("Client.uid").getSize();
      //      System.out.println("\nTotal indexed objects after the test: " + indexedItems);

    } finally {
      graph.close();
    }
  }
}
