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
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.distributed.task.ODistributedRecordLockedException;
import com.orientechnologies.orient.setup.ServerRun;
import java.util.Date;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;

/** Test distributed TX */
public abstract class AbstractDistributedConcurrentTxTest extends AbstractDistributedWriteTest {
  //  protected ODatabasePool pool;
  protected ORID v;
  protected AtomicLong lockExceptions = new AtomicLong(0l);
  protected boolean expectedConcurrentException = true;

  class TxWriter implements Callable<Void> {
    private final int serverId;

    public TxWriter(final int iServerId) {
      serverId = iServerId;
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(serverId);

      for (int i = 0; i < count; i++) {
        final ODatabaseDocument graph = getDatabase(serverId);

        final OVertex localVertex = getVertex(graph, v);

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

          int retry = 0;
          boolean success = false;
          for (; retry < 200; ++retry) {
            try {
              updateVertex(localVertex);
              graph.commit();
              OLogManager.instance().info(this, "Success count %d retry %d", i, retry);
              success = true;
              break;

            } catch (ODistributedRecordLockedException e) {
              lockExceptions.incrementAndGet();
              OLogManager.instance()
                  .info(this, "increment lockExceptions %d", lockExceptions.get());

            } catch (ONeedRetryException e) {
              OLogManager.instance().debug(this, "Concurrent Exceptions " + e);

            } catch (Exception e) {
              graph.rollback();
              throw e;
            }

            Thread.sleep(10 + new Random().nextInt(500));

            localVertex.reload();

            OLogManager.instance()
                .info(
                    this,
                    "Retry %d with reloaded vertex v=%d",
                    retry,
                    localVertex.getRecord().getVersion());
          }

          Assert.assertTrue(
              "Unable to complete the transaction (last="
                  + i
                  + "/"
                  + count
                  + "), even after "
                  + retry
                  + " retries",
              success);

        } catch (InterruptedException e) {
          System.out.println("Writer received interrupt (db=" + graph.getURL());
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          System.out.println("Writer received exception (db=" + graph.getURL());
          e.printStackTrace();
          break;
        } finally {
          graph.close();
        }
      }

      System.out.println(
          "\nWriter " + name + " END. count = " + count + " lockExceptions: " + lockExceptions);
      return null;
    }
  }

  protected AbstractDistributedConcurrentTxTest() {
    count = 200;
    writerCount = 3;
  }

  protected void onAfterExecution() {

    final long totalLockExceptions = lockExceptions.get();

    if (expectedConcurrentException) {
      Assert.assertTrue("lockExceptions are " + totalLockExceptions, totalLockExceptions > 0);
    } else {
      Assert.assertTrue("lockExceptions are " + totalLockExceptions, totalLockExceptions == 0);
    }
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

    OClass person = graph.getClass("Person");
    person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    OClass customer = graph.createClass("Customer", person.getName());
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = graph.createClass("Provider", person.getName());
    provider.createProperty("totalPurchased", OType.DECIMAL);

    //    pool = new ODatabasePool(graph.getURL(), "admin", "admin",
    // OrientDBConfig.defaultConfig());

    v = createVertex(graph, 0, 0, 0).getIdentity();
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, ServerRun server) {
    return new TxWriter(serverId);
  }

  protected OVertex createVertex(ODatabaseDocument graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;

    final Object result =
        graph
            .command(
                new OCommandSQL(
                    "create vertex Provider content {'id': '"
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
                        + "', 'saved': 0}"))
            .execute();

    return getVertex((ODocument) result);
  }

  protected void updateVertex(OVertex v) {
    v.setProperty("saved", ((Integer) v.getProperty("saved")) + 1);
    v.save();
  }
}
