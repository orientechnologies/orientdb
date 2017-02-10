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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import org.junit.Assert;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test distributed TX
 */
public abstract class AbstractServerClusterGraphTest extends AbstractServerClusterInsertTest {
  protected ODatabasePool pool;
  protected ORID          rootVertexId;
  protected Object lock = new Object();

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
          ODatabaseDocument graph = pool.acquire();
          graph.begin();
          try {
            OVertex root = createVertex(graph, serverId, threadId, 0);
            root.save();

            rootVertexId = root.getIdentity();
          } finally {
            graph.close();
          }
        }
      }

      int itemInTx = 0;
      final ODatabaseDocument graph = pool.acquire();
      try {
        for (int i = 1; i <= count; i++) {
          if (i % 100 == 0)
            System.out.println("\nWriter " + databaseUrl + " managed " + i + "/" + count + " vertices so far");

          for (int retry = 0; retry < 100; retry++) {
            try {
              OVertex person = createVertex(graph, serverId, threadId, i);
              updateVertex(graph, person);
              checkVertex(graph, person);

              final OVertex root = graph.load(rootVertexId);
              root.addEdge(person).save();

              // checkIndex(database, (String) person.field("name"), person.getIdentity());

              if (i % 10 == 0 || i == count) {
                graph.commit();
                graph.begin();
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
              graph.begin();
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
        graph.close();
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }
  }

  @Override
  protected void computeExpected(int servers) {
    expected = writerCount * count * servers + baseCount + 1;
  }

  protected void onAfterExecution() {
    pool.close();
  }

  @Override
  protected void onAfterDatabaseCreation(final ODatabaseDocument graph) {
    System.out.println("Creating graph schema...");

    // CREATE BASIC SCHEMA
    OClass personClass = graph.createVertexClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.STRING);

    OClass person = graph.createVertexClass("Person");
    idx = person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    OClass customer = graph.createClass("Customer", person.getName());
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = graph.createClass("Provider", person.getName());
    provider.createProperty("totalPurchased", OType.DECIMAL);

    pool = new ODatabasePool(graph.getURL(), "admin", "admin", OrientDBConfig.defaultConfig());
    setFactorySettings(pool);
  }

  protected void setFactorySettings(ODatabasePool pool) {
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new TxWriter(serverId, threadId, databaseURL);
  }

  protected OVertex createVertex(ODatabaseDocument graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    OVertex result = graph.newVertex("Person");
    result.setProperty("id", UUID.randomUUID().toString());
    result.setProperty("name", "Billy" + uniqueId);
    result.setProperty("surname", "Mayes" + uniqueId);
    result.setProperty("birthday", new Date());
    result.setProperty("children", uniqueId);
    result.save();
    return result;
  }

  protected void updateVertex(ODatabaseDocument graph, OVertex v) {
    v.setProperty("updated", true);
    v.save();
  }

  protected void checkVertex(ODatabaseDocument graph, OVertex v) {
    v.reload();
    Assert.assertEquals(v.getProperty("updated"), Boolean.TRUE);
  }
}
