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

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.*;
import org.junit.Assert;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test distributed TX
 */
public abstract class AbstractServerClusterGraphTest extends AbstractServerClusterInsertTest {
  protected OrientGraphFactory factory;
  protected ORID               rootVertexId;
  protected Object             lock = new Object();

  class TxWriter implements Callable<Void> {
    private final String databaseUrl;
    private final int    serverId;
    private final int    threadId;

    public TxWriter(final int iServerId, final int iThreadId, final String db) {
      serverId = iServerId;
      threadId = iThreadId;
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(serverId);

      synchronized (lock) {
        if (rootVertexId == null) {
          // ONLY THE FIRST TIME CREATE THE ROOT
          OrientGraph graph = factory.getTx();
          try {
            OrientVertex root = createVertex(graph, serverId, threadId, 0);
            rootVertexId = root.getIdentity();
          } finally {
            graph.shutdown();
          }
        }
      }

      int itemInTx = 0;
      final OrientBaseGraph graph = factory.getTx();
      try {
        for (int i = 1; i <= count; i++) {
          if (i % 100 == 0)
            System.out.println("\nWriter " + databaseUrl + " managed " + i + "/" + count + " vertices so far");

          for (int retry = 0; retry < 100; retry++) {
            try {
              OrientVertex person = createVertex(graph, serverId, threadId, i);
               updateVertex(graph, person);
               checkVertex(graph, person);

              final OrientVertex root = graph.getVertex(rootVertexId);
              root.addEdge("E", person);

              // checkIndex(database, (String) person.field("name"), person.getIdentity());

              if (i % 10 == 0 || i == count) {
                graph.commit();
                itemInTx = 0;
              } else
                itemInTx++;

              break;

            } catch (ONeedRetryException e) {
              graph.rollback();
              i -= itemInTx;
              itemInTx = 0;
              // RETRY
            } catch (Exception e) {
              graph.rollback();
              throw e;
            }
          }

          if (delayWriter > 0)
            Thread.sleep(delayWriter);
        }
      } catch (InterruptedException e) {
        System.out.println("Writer received interrupt (db=" + databaseUrl);
        Thread.currentThread().interrupt();
      } catch (Exception e) {
        System.out.println("Writer received exception (db=" + databaseUrl);
        e.printStackTrace();
      } finally {
        runningWriters.countDown();
        graph.shutdown();
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }
  }

  @Override
  protected void computeExpected(int serverId) {
    expected = writerCount * count * serverId + baseCount + 1;
  }

  protected void onAfterExecution() {
    factory.close();
  }

  @Override
  protected void onAfterDatabaseCreation(final OrientBaseGraph graph) {
    System.out.println("Creating graph schema...");

    // CREATE BASIC SCHEMA
    OrientVertexType personClass = graph.createVertexType("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.STRING);

    OrientVertexType person = graph.getVertexType("Person");
    idx = person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    OrientVertexType customer = graph.createVertexType("Customer", person);
    customer.createProperty("totalSold", OType.DECIMAL);

    OrientVertexType provider = graph.createVertexType("Provider", person);
    provider.createProperty("totalPurchased", OType.DECIMAL);

    factory = new OrientGraphFactory(graph.getRawGraph().getURL(), "admin", "admin", false);
    setFactorySettings(factory);
    factory.setStandardElementConstraints(false);
  }

  protected void setFactorySettings(OrientGraphFactory factory) {
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new TxWriter(serverId, threadId, databaseURL);
  }

  protected OrientVertex createVertex(OrientBaseGraph graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    return graph.addVertex("class:Person", "id", UUID.randomUUID().toString(), "name", "Billy" + uniqueId, "surname",
        "Mayes" + uniqueId, "birthday", new Date(), "children", uniqueId);
  }

  protected void updateVertex(OrientBaseGraph graph, OrientVertex v) {
    v.setProperty("updated", true);
  }

  protected void checkVertex(OrientBaseGraph graph, OrientVertex v) {
    v.reload();
    Assert.assertEquals(v.getProperty("updated"), Boolean.TRUE);
  }
}
