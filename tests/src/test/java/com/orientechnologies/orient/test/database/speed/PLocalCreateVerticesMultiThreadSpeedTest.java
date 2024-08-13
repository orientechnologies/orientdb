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

import com.orientechnologies.common.test.SpeedTestMultiThreads;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.test.database.base.OrientMultiThreadTest;
import com.orientechnologies.orient.test.database.base.OrientThreadTest;
import org.testng.Assert;
import org.testng.annotations.Test;

@Test(enabled = false)
public class PLocalCreateVerticesMultiThreadSpeedTest extends OrientMultiThreadTest {
  protected static final String URL = "embedded:target/databases";
  protected static final OrientDB factory = new OrientDB(URL, OrientDBConfig.defaultConfig());
  protected long foundObjects;

  @Test(enabled = false)
  public static class CreateObjectsThread extends OrientThreadTest {
    private ODatabaseSession graph;

    public CreateObjectsThread(final SpeedTestMultiThreads parent, final int threadId) {
      super(parent, threadId);
    }

    @Override
    public void init() {
      factory
          .execute(
              "create database graphspeedtest plocal users(admin identified by 'adminpwd' role"
                  + " admin)")
          .close();
      graph = factory.open("graphspeedtest", "admin", "adminpwd");
    }

    public void cycle() {
      OVertex vertex = graph.newVertex("Client");
      vertex.setProperty("uid", "" + currentThreadId() + "_" + data.getCyclesDone());
      graph.save(vertex);
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

  public PLocalCreateVerticesMultiThreadSpeedTest() {
    super(1000000, 4, CreateObjectsThread.class);
  }

  public static void main(String[] iArgs) throws InstantiationException, IllegalAccessException {
    // System.setProperty("url", "memory:test");
    PLocalCreateVerticesMultiThreadSpeedTest test = new PLocalCreateVerticesMultiThreadSpeedTest();
    test.data.go(test);
  }

  @Override
  public void init() {
    factory
        .execute(
            "create database graphspeedtest plocal users(admin identified by 'adminpwd' role"
                + " admin)")
        .close();
    final ODatabaseSession graph = factory.open("graphspeedtest", "admin", "adminpwd");
    try {
      if (!graph.getMetadata().getSchema().existsClass("Client")) {
        final OClass clientType = graph.createVertexClass("Client");

        final OProperty property = clientType.createProperty("uid", OType.STRING);
        property.createIndex(OClass.INDEX_TYPE.UNIQUE_HASH_INDEX);

        // CREATE ONE CLUSTER PER THREAD
        for (int i = 0; i < getThreads(); ++i) {
          System.out.println("Creating cluster: client_" + i + "...");
          clientType.addCluster("client_" + i);
        }

        foundObjects = 0;
      } else foundObjects = graph.countClass("Client");

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
      System.out.println("Created " + (total - foundObjects));
      Assert.assertEquals(total - foundObjects, threadCycles);

      final long indexedItems =
          ((ODatabaseDocumentInternal) graph)
              .getMetadata()
              .getIndexManagerInternal()
              .getIndex((ODatabaseDocumentInternal) graph, "Client.uid")
              .getInternal()
              .size();
      System.out.println("\nTotal indexed objects after the test: " + indexedItems);

    } finally {
      graph.close();
    }
  }
}
