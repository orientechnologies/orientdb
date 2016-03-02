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

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Assert;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test distributed TX
 */
public abstract class AbstractServerClusterGraphTest extends AbstractServerClusterInsertTest {
  protected OrientGraphFactory factory;

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

      for (int i = 0; i < count; i++) {
        final OrientGraph graph = factory.getTx();
        try {
          if ((i + 1) % 100 == 0)
            System.out.println("\nWriter " + databaseUrl + " managed " + (i + 1) + "/" + count + " vertices so far");

          try {
            OrientVertex person = createVertex(graph, serverId, threadId, i);
            updateVertex(graph, person);
            checkVertex(graph, person);
            // checkIndex(database, (String) person.field("name"), person.getIdentity());

            graph.commit();

            Assert.assertTrue(person.getIdentity().isPersistent());
          } catch (Exception e) {
            graph.rollback();
            throw e;
          }

          if (delayWriter > 0)
            Thread.sleep(delayWriter);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + databaseUrl);
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          System.out.println("Writer received exception (db=" + databaseUrl);
          e.printStackTrace();
          break;
        } finally {
          runningWriters.countDown();
          graph.shutdown();
        }
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }
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

    factory = new OrientGraphFactory(graph.getRawGraph().getURL(), "admin", "admin");
    factory.setStandardElementConstraints(false);
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new TxWriter(serverId, threadId, databaseURL);
  }

  protected OrientVertex createVertex(OrientGraph graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    return graph.addVertex("class:Person", "id", UUID.randomUUID().toString(), "name", "Billy" + uniqueId, "surname", "Mayes"
        + uniqueId, "birthday", new Date(), "children", uniqueId);
  }

  protected void updateVertex(OrientGraph graph, OrientVertex v) {
    v.setProperty("updated", true);
  }

  protected void checkVertex(OrientGraph graph, OrientVertex v) {
    v.reload();
    Assert.assertEquals(v.getProperty("updated"), Boolean.TRUE);
  }
}
