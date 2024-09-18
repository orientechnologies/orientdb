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

package com.orientechnologies.orient.server.distributed;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

/** Test distributed TX */
public abstract class AbstractServerClusterSQLGraphTest extends AbstractServerClusterInsertTest {

  class TxWriter implements Callable<Void> {
    private final int serverId;
    private final int threadId;

    public TxWriter(final int iServerId, final int iThreadId) {
      serverId = iServerId;
      threadId = iThreadId;
    }

    @Override
    public Void call() throws Exception {
      ODatabasePool pool =
          new ODatabasePool(
              serverInstance.get(serverId).getServerInstance().getContext(),
              getDatabaseName(),
              "admin",
              "admin",
              OrientDBConfig.defaultConfig());
      String name = Integer.toString(serverId);

      try {
        for (int i = 0; i < count; i += 2) {
          final ODatabaseDocument graph = pool.acquire();
          try {
            if ((i + 1) % 100 == 0)
              System.out.println(
                  "\nWriter "
                      + graph.getURL()
                      + " managed "
                      + (i + 1)
                      + "/"
                      + count
                      + " vertices so far");

            try {
              OVertex person1 = createVertex(graph, serverId, threadId, i);
              OVertex person2 = createVertex(graph, serverId, threadId, i + 1);

              OEdge knows = createEdge(graph, person1, person2);

              assertEquals(knows.getFrom(), person1.getIdentity());
              assertEquals(knows.getTo(), person2.getIdentity());

              graph.commit();

              updateVertex(graph, person1);
              checkVertex(graph, person1);
              assertTrue(person1.getIdentity().isPersistent());

              updateVertex(graph, person2);
              checkVertex(graph, person2);
              assertTrue(person2.getIdentity().isPersistent());
            } catch (Exception e) {
              graph.rollback();
              throw e;
            }

            if (delayWriter > 0) Thread.sleep(delayWriter);

          } catch (InterruptedException e) {
            System.out.println("Writer received interrupt (db=" + graph.getURL());
            Thread.currentThread().interrupt();
            break;
          } catch (Exception e) {
            System.out.println("Writer received exception (db=" + graph.getURL());
            e.printStackTrace();
            break;
          } finally {
            runningWriters.countDown();
            graph.activateOnCurrentThread();
            graph.close();
          }
        }
      } finally {
        pool.close();
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }
  }

  protected void onAfterExecution() {}

  @Override
  protected void onAfterDatabaseCreation(final ODatabaseDocument graph) {
    System.out.println("Creating graph schema...");

    // CREATE BASIC SCHEMA
    OClass personClass = graph.createVertexClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.STRING);

    OClass person = graph.getClass("Person");
    idx = person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    OClass customer = graph.createClass("Customer", person.getName());
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = graph.createClass("Provider", person.getName());
    provider.createProperty("totalPurchased", OType.DECIMAL);

    OClass knows = graph.createEdgeClass("Knows");
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, ServerRun server) {
    return new TxWriter(serverId, threadId);
  }

  protected OVertex createVertex(ODatabaseDocument graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    try (final OResultSet result =
        graph.command(
            "create vertex Person content {'id': '"
                + UUID.randomUUID().toString()
                + "', 'name': 'Billy"
                + uniqueId
                + "', 'surname': 'Mayes"
                + uniqueId
                + "', 'birthday': '"
                + ODatabaseRecordThreadLocal.instance()
                    .get()
                    .getStorage()
                    .getConfiguration()
                    .getDateFormatInstance()
                    .format(new Date())
                + "', 'children': '"
                + uniqueId
                + "'}")) {

      return result.next().getVertex().get();
    }
  }

  protected OEdge createEdge(ODatabaseDocument graph, OVertex v1, OVertex v2) {
    try (OResultSet result =
        graph.command("create edge knows from " + v1.getIdentity() + " to " + v2.getIdentity())) {

      return result.next().getEdge().get();
    }
  }

  protected void updateVertex(ODatabaseDocument graph, OVertex v) {
    graph.command("update " + v.getIdentity() + " set updated = true").close();
  }

  protected void checkVertex(ODatabaseDocument graph, OVertex v) {
    try (final OResultSet result = graph.query("select from " + v.getIdentity())) {
      assertTrue(result.hasNext());
      final OVertex vertex = result.next().getVertex().get();
      vertex.reload();

      assertTrue(Boolean.TRUE.equals(vertex.getProperty("updated")));
    }
  }
}
