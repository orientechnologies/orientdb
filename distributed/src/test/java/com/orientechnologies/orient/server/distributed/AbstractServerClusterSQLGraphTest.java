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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientEdge;
import com.tinkerpop.blueprints.impls.orient.OrientEdgeType;
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
public abstract class AbstractServerClusterSQLGraphTest extends AbstractServerClusterInsertTest {
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

      for (int i = 0; i < count; i += 2) {
        final OrientGraph graph = factory.getTx();
        try {
          if ((i + 1) % 100 == 0)
            System.out.println("\nWriter " + databaseUrl + " managed " + (i + 1) + "/" + count + " vertices so far");

          try {
            OrientVertex person1 = createVertex(graph, serverId, threadId, i);
            OrientVertex person2 = createVertex(graph, serverId, threadId, i + 1);

            OrientEdge knows = createEdge(graph, person1, person2);

            Assert.assertEquals(knows.getOutVertex(), person1.getIdentity());
            Assert.assertEquals(knows.getInVertex(), person2.getIdentity());

            graph.commit();

            updateVertex(graph, person1);
            checkVertex(graph, person1);
            Assert.assertTrue(person1.getIdentity().isPersistent());

            updateVertex(graph, person2);
            checkVertex(graph, person2);
            Assert.assertTrue(person2.getIdentity().isPersistent());
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

    OrientEdgeType knows = graph.createEdgeType("Knows");

    factory = new OrientGraphFactory(graph.getRawGraph().getURL(), "admin", "admin");
    factory.setStandardElementConstraints(false);
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new TxWriter(serverId, threadId, databaseURL);
  }

  protected OrientVertex createVertex(OrientGraph graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    final Object result = graph.command(
        new OCommandSQL("create vertex Person content {'id': '" + UUID.randomUUID().toString() + "', 'name': 'Billy" + uniqueId
            + "', 'surname': 'Mayes" + uniqueId + "', 'birthday': '"
            + ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance().format(new Date())
            + "', 'children': '" + uniqueId + "'}")).execute();
    return (OrientVertex) result;
  }

  protected OrientEdge createEdge(OrientGraph graph, OrientVertex v1, OrientVertex v2) {
    final Iterable<OrientEdge> result = graph.command(
        new OCommandSQL("create edge knows from " + v1.getIdentity() + " to " + v2.getIdentity())).execute();
    return result.iterator().next();
  }

  protected void updateVertex(OrientGraph graph, OrientVertex v) {
    graph.command(new OCommandSQL("update " + v.getIdentity() + " set updated = true")).execute();
  }

  protected void checkVertex(OrientGraph graph, OrientVertex v) {
    final Iterable<OrientVertex> result = graph.command(new OCommandSQL("select from " + v.getIdentity())).execute();
    Assert.assertTrue(result.iterator().hasNext());

    final OrientVertex vertex = result.iterator().next();
    vertex.reload();

    Assert.assertTrue((Boolean) vertex.getProperty("updated"));
  }
}
