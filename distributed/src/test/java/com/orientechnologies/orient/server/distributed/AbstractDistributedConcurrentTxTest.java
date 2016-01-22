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
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;
import com.tinkerpop.blueprints.impls.orient.OrientVertexType;
import org.junit.Assert;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Test distributed TX
 */
public abstract class AbstractDistributedConcurrentTxTest extends AbstractDistributedWriteTest {
  protected OrientGraphFactory factory;
  protected ORID               v;
  protected AtomicLong         lockExceptions              = new AtomicLong(0l);
  protected boolean            expectedConcurrentException = true;

  class TxWriter implements Callable<Void> {
    private final String databaseUrl;
    private final int    serverId;

    public TxWriter(final int iServerId, final String db) {
      serverId = iServerId;
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(serverId);

      for (int i = 0; i < count; i += 2) {
        final OrientGraph graph = factory.getTx();

        final OrientVertex localVertex = graph.getVertex(v);

        try {
          if ((i + 1) % 100 == 0)
            System.out.println("\nWriter " + databaseUrl + " managed " + (i + 1) + "/" + count + " vertices so far");

          boolean success = false;
          for (int retry = 0; retry < 100; ++retry) {
            try {
              updateVertex(graph, localVertex);
              graph.commit();
              success = true;
              break;

            } catch (OConcurrentModificationException e) {
              graph.rollback();

            } catch (ODistributedRecordLockedException e) {
              lockExceptions.incrementAndGet();
              graph.rollback();

            } catch (Exception e) {
              graph.rollback();
              throw e;
            }
          }

          Assert.assertTrue(success);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + databaseUrl);
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          System.out.println("Writer received exception (db=" + databaseUrl);
          e.printStackTrace();
          break;
        } finally {
          graph.shutdown();
        }
      }

      System.out.println("\nWriter " + name + " END. count = " + count + " lockExceptions: " + lockExceptions);

      if (expectedConcurrentException)
        Assert.assertTrue(lockExceptions.get() > 0);
      else
        Assert.assertTrue(lockExceptions.get() == 0);

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
    person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    OrientVertexType customer = graph.createVertexType("Customer", person);
    customer.createProperty("totalSold", OType.DECIMAL);

    OrientVertexType provider = graph.createVertexType("Provider", person);
    provider.createProperty("totalPurchased", OType.DECIMAL);

    factory = new OrientGraphFactory(graph.getRawGraph().getURL(), "admin", "admin");
    factory.setStandardElementConstraints(false);

    v = createVertex(graph, 0, 0, 0).getIdentity();
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new TxWriter(serverId, databaseURL);
  }

  protected OrientVertex createVertex(OrientBaseGraph graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    final Object result = graph.command(
        new OCommandSQL("create vertex Provider content {'id': '" + UUID.randomUUID().toString() + "', 'name': 'Billy" + uniqueId
            + "', 'surname': 'Mayes" + uniqueId + "', 'birthday': '"
            + ODatabaseRecordThreadLocal.INSTANCE.get().getStorage().getConfiguration().getDateFormatInstance().format(new Date())
            + "', 'children': '" + uniqueId + "', 'saved': 0}")).execute();
    return (OrientVertex) result;
  }

  protected void updateVertex(OrientGraph graph, OrientVertex v) {
    v.setProperty("saved", ((Integer) v.getProperty("saved")) + 1);
  }
}
