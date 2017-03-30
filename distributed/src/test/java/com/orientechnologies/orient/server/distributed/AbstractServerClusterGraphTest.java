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
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Assert;

import java.util.Date;
import java.util.UUID;
import java.util.concurrent.Callable;

/**
 * Test distributed TX
 */
public abstract class AbstractServerClusterGraphTest extends AbstractServerClusterInsertTest {
//  protected ODatabasePool pool;
  protected ORID          rootVertexId;
  protected Object lock = new Object();

  class TxWriter implements Callable<Void> {
    private final String databaseUrl;
    private final int    serverId;
    private final int    threadId;
    
    private ODatabasePool dbPool;

    public TxWriter(final int iServerId, final int iThreadId, final String db) {
      serverId = iServerId;
      threadId = iThreadId;
      databaseUrl = db;
      
      dbPool = new ODatabasePool(serverInstance.get(iServerId).getServerInstance().getContext(), getDatabaseName(), "admin", "admin", OrientDBConfig.defaultConfig());
      setFactorySettings(dbPool);

System.out.println("------ new TxWriter() serverId = " + iServerId + ", threadId = " + iThreadId);
    }

    @Override
    public Void call() throws Exception {
      String name = Integer.toString(serverId);

System.out.println("------ TxWriter().call serverId = " + serverId + ", threadId = " + threadId);

      synchronized (lock) {
        if (rootVertexId == null) {
          // ONLY THE FIRST TIME CREATE THE ROOT
System.out.println("------ pool.acquire()");
          ODatabaseDocument graph = dbPool.acquire();
System.out.println("------ graph.begin()");
          try {
            graph.begin();
System.out.println("------ createVertex()");
            OVertex root = createVertex(graph, serverId, threadId, 0);
            root.save();
            
            graph.commit();

            rootVertexId = root.getIdentity();
            
System.out.println("------ rootVertexId =" + rootVertexId);
            
          } finally {
            graph.close();
          }
        }
      }

System.out.println("------ ");

      int itemInTx = 0;
      final ODatabaseDocument graph = dbPool.acquire();
      try {
        graph.begin();
      	
        for (int i = 1; i <= count; i++) {
          if (i % 100 == 0)
            System.out.println("\nWriter " + databaseUrl + " managed " + i + "/" + count + " vertices so far");

          for (int retry = 0; retry < 100; retry++) {
            try {
              OVertex person = createVertex(graph, serverId, threadId, i);
              updateVertex(graph, person);
              checkVertex(graph, person);

              ODocument rootDoc = graph.load(rootVertexId);
              
if(rootDoc == null) System.out.println("!!!!!!!! rootDoc is null");
  
              final OVertex root = rootDoc.asVertex().get();
              
if(root == null) System.out.println("!!!!!!!! rootis null");

              
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

System.out.println("---------- ONeedRetryException for serverId = " + serverId + ", threadId = " + threadId);
System.out.println("---------- ONeedRetryException e = " + e.getMessage());
              graph.rollback();
              graph.begin();
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
        dbPool.close();
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }
  }

  @Override
  protected void computeExpected(int servers) {
    expected = writerCount * count * servers + baseCount + 1;
  }

  @Override
  protected void onBeforeExecution() {
  }

  @Override
  protected void onAfterExecution() {
//    pool.close();
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
    idx = person.createIndex("Person.name", OClass.INDEX_TYPE.UNIQUE, "name");

    OClass customer = graph.createClass("Customer", personClass.getName());
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = graph.createClass("Provider", personClass.getName());
    provider.createProperty("totalPurchased", OType.DECIMAL);    
  }

  protected void setFactorySettings(ODatabasePool pool) {
  }

  @Override
  protected Callable<Void> createWriter(final int serverId, final int threadId, String databaseURL) {
    return new TxWriter(serverId, threadId, databaseURL);
  }

  protected OVertex createVertex(ODatabaseDocument graph, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;
    
System.out.println("--createVertex() uniqueId = " + uniqueId);    

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
