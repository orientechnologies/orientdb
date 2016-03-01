/*
 * Copyright 2015 OrientDB LTD (info--at--orientdb.com)
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

package com.orientechnologies.orient.server.distributed.scenariotest;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraphFactory;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * It represents an abstract scenario test with sharding on the cluster.
 */

public class AbstractShardingScenarioTest extends AbstractScenarioTest {


  protected OrientVertex loadVertex(OrientBaseGraph graph, String shardName, int serverId, int threadId, int i) {

    List<OrientVertex> result = null;

    try {
      final String uniqueId = shardName + "-s" + serverId + "-t" + threadId + "-v" + i;
      Iterable<Vertex> it = graph.command(new OCommandSQL("select from Client where name = '" + uniqueId + "'")).execute();
      result = new LinkedList<OrientVertex>();

      for (Vertex v : it) {
        result.add((OrientVertex) v);
      }

      if (result.size() == 0)
        junit.framework.Assert.assertTrue("No record found with name = '" + uniqueId + "'!", false);
      else if (result.size() > 1)
        junit.framework.Assert.assertTrue(result.size() + " records found with name = '" + uniqueId + "'!", false);

    } catch (Exception e) {
      e.printStackTrace();
    }

    if(result.size() == 0)
      return null;

    return result.get(0);
  }



    /*
     * It executes multiple writes using different concurrent writers (as specified by the value writerCount)
     * on all the servers present in the collection passed as parameter in a specific cluster-shards.
     * Each write performs a vertex insert and some update and check operations on it.
     * Vertex name: <shardName>-s<serverId>-t<threadId>-<recordId>
     */

  protected void executeMultipleWritesOnShards(List<ServerRun> executeOnServers, String storageType) throws InterruptedException, ExecutionException {

    System.out.println("Creating Writers threads...");

    final ExecutorService writerExecutors = Executors.newCachedThreadPool();
    final ExecutorService readerExecutors = Executors.newCachedThreadPool();

    runningWriters = new CountDownLatch(executeOnServers.size() * writerCount);

    String shardName = "client_";
    int serverId = 0;
    int threadId = 0;
    List<Callable<Void>> writerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : executeOnServers) {
      if (server.isActive()) {
        shardName += server.getServerInstance().getDistributedManager().getLocalNodeName();
        for (int j = 0; j < writerCount; j++) {
          Callable writer = null;
          if(storageType.equals("plocal")) {
            writer = new ShardWriter(serverId, shardName, threadId++, getPlocalDatabaseURL(server));
          }
          else if(storageType.equals("remote")) {
            writer = new ShardWriter(serverId, shardName, threadId++, getPlocalDatabaseURL(server));
          }
          writerWorkers.add(writer);
        }
      }
      serverId++;
      shardName = "client_";
    }

    expected = writerCount * count * serverId + baseCount;

    List<Future<Void>> futures = writerExecutors.invokeAll(writerWorkers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    writerExecutors.shutdown();
    junit.framework.Assert.assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished.");

    // checking inserted vertices
    OrientBaseGraph graph;
    OrientGraphFactory graphFactory;

    // checking total amount of records (map-reduce aggregation)
    graphFactory = new OrientGraphFactory("plocal:target/server0/databases/" + getDatabaseName());
    graph = graphFactory.getNoTx();
    try {
      OResultSet<ODocument> clients = new OCommandSQL("select from Client").execute();
      final int total = clients.size();
      junit.framework.Assert.assertEquals(expected, total);
    } finally {
      graph.getRawGraph().close();
    }

    serverId = 0;
    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        graphFactory = new OrientGraphFactory("plocal:target/server" + serverId + "/databases/" + getDatabaseName());
        graph = graphFactory.getNoTx();
        try {
          String sqlCommand = "select from cluster:client_" + server.getServerInstance().getDistributedManager().getLocalNodeName();
          OResultSet<ODocument> clients = new OCommandSQL(sqlCommand).execute();
          final int total = clients.size();
          junit.framework.Assert.assertEquals(500, total);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          graph.getRawGraph().close();
        }
      }
      serverId++;
    }

    // checking indexes
    //        serverId = 0;
    //        for (ServerRun server : serverInstance) {
    //            if (server.isActive()) {
    //                graphFactory = new OrientGraphFactory("plocal:target/server" + serverId + "/databases/" + getDatabaseName());
    //                graph = graphFactory.getNoTx();
    //                try {
    //                    final long indexSize = graph.getRawGraph().getMetadata().getIndexManager().getIndex("Client.name").getSize();
    //
    //                    if (indexSize != count) {
    //                        // ERROR: DUMP ALL THE RECORDS
    //                        List<ODocument> result = graph.command(new OCommandSQL("select from index:Client.name")).execute();
    //                        int i = 0;
    //                        for (ODocument d : result) {
    //                            System.out.println((i++) + ": " + ((OIdentifiable) d.field("rid")).getRecord());
    //                        }
    //                    }
    //
    //                    junit.framework.Assert.assertEquals(count, indexSize);
    //
    //                    System.out.println("From metadata: indexes " + indexSize + " items");
    //
    //                    List<ODocument> result = graph.command(new OCommandSQL("select count(*) from index:Client.name")).execute();
    //                    junit.framework.Assert.assertEquals(count, ((Long) result.get(0).field("count")).longValue());
    //
    //                    System.out.println("From sql: indexes " + indexSize + " items");
    //                } finally {
    //                    graph.getRawGraph().close();
    //                }
    //            }
    //            serverId++;
    //        }
  }


  // checks the consistency in the cluster after the writes in a no-replica sharding scenario
  protected void checkWritesWithShardinNoReplica(List<ServerRun> checkConsistencyOnServers, List<ServerRun> writerServer) {

    String checkOnServer = "";
    for(ServerRun server: checkConsistencyOnServers) {
      checkOnServer += server.getServerInstance().getDistributedManager().getLocalNodeName() + ",";
    }
    checkOnServer = checkOnServer.substring(0,checkOnServer.length()-1);

    String writtenServer = "";
    for(ServerRun server: writerServer) {
      writtenServer += server.getServerInstance().getDistributedManager().getLocalNodeName() + ",";
    }
    writtenServer = writtenServer.substring(0,writtenServer.length()-1);

    List<OrientBaseGraph> dbs = new LinkedList<OrientBaseGraph>();

    OrientGraphFactory localFactory = null;

    for(ServerRun server: checkConsistencyOnServers) {
      localFactory = new OrientGraphFactory(getPlocalDatabaseURL(server));
      dbs.add(localFactory.getNoTx());
    }

    Map<Integer, Integer> serverIndex2thresholdThread = new LinkedHashMap<Integer, Integer>();
    Map<Integer, String> serverIndex2serverName = new LinkedHashMap<Integer, String>();

    int lastThread = 0;
    int serverIndex = 0;

    for(ServerRun server: writerServer) {
      serverIndex2thresholdThread.put(serverIndex, lastThread+5);
      serverIndex++;
      lastThread += 5;
    }

    serverIndex = 0;

    for(ServerRun server: writerServer) {
      serverIndex2serverName.put(serverIndex, server.getServerInstance().getDistributedManager().getLocalNodeName());
      serverIndex++;
    }

    List<OrientVertex> verticesToCheck = new LinkedList<OrientVertex>();

    super.banner("Checking consistency among servers...\nChecking on servers {" + checkOnServer + "} that all the vertices written on {" + writtenServer + "} are consistent.");

    try {

      int index = 0;
      String serverName = null;

      for(int serverId: serverIndex2thresholdThread.keySet()) {

        serverName = serverIndex2serverName.get(serverId);
        System.out.println("Checking records originally inserted on server " + serverName + "...");

        String clusterName = "client_" + serverName;

        // checking records inserted on server0
        int i;
        if(serverId == 0)
          i = 0;
        else
          i = serverIndex2thresholdThread.get(serverId-1);

        while (i < serverIndex2thresholdThread.get(serverId)) {
          for (int j = 0; j < 100; j++) {

            // load records to compare
            for(OrientBaseGraph db: dbs) {
              verticesToCheck.add(loadVertex(db, clusterName, serverId, i, j + baseCount));
            }

            // checking that record is present on each server db
            OrientVertex currentVertex = null;
            int k=0;
            while(k < verticesToCheck.size()) {
              assertTrue(verticesToCheck.get(k) != null);
              k++;
            }

            // checking that all the records have the same version and values (each record is equal to the next one)
            k = 0;
            while(k <= verticesToCheck.size() -2) {
              assertEquals(verticesToCheck.get(k).getProperty("@version"),verticesToCheck.get(k+1).getProperty("@version"));
              assertEquals(verticesToCheck.get(k).getProperty("name"), verticesToCheck.get(k+1).getProperty("name"));
              assertEquals(verticesToCheck.get(k).getProperty("updated"), verticesToCheck.get(k+1).getProperty("updated"));
              k++;
            }
            verticesToCheck.clear();
          }
          i++;
        }

        System.out.println("All records originally inserted on server " + serverName + " in the cluster " + clusterName + " available in the shard.");
        index++;
      }

    } catch(Exception e) {
      e.printStackTrace();
    } finally {

      for(OrientBaseGraph db: dbs) {
        ODatabaseRecordThreadLocal.INSTANCE.set(db.getRawGraph());
        db.getRawGraph().close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }
  }


    /*
     * A Callable task that inserts many vertices as indicated by the count variable on the specified server and cluster (shard).
     */

  protected class ShardWriter implements Callable<Void> {
    protected final String databaseUrl;
    protected int          serverId;
    protected final String shardName;
    protected int          threadId;

    protected ShardWriter(final int iServerId, final String shardName, final int iThreadId, final String db) {
      serverId = iServerId;
      this.shardName = shardName;
      threadId = iThreadId;
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {

      try {

        String name = Integer.toString(threadId);
        OrientGraphFactory graphFactory = new OrientGraphFactory("plocal:target/server" + serverId + "/databases/" + getDatabaseName());
        OrientBaseGraph graph = graphFactory.getNoTx();

        for (int i = 0; i < count; i++) {

          try {
            final int id = baseCount + i;

            int retry = 0;

            for (retry = 0; retry < maxRetries; retry++) {

              try {
                final OrientVertex client = createVertex(graph, id);

                updateVertex(graph, id);
                checkVertex(graph, id);
                //                            checkIndex(graph, (String) client.getProperty("name"), client.getIdentity());

                if ((i + 1) % 100 == 0)
                  System.out.println("\nWriter " + graph.getRawGraph().getURL() + " managed " + (i + 1) + "/" + count + " records so far");

                if (delayWriter > 0)
                  Thread.sleep(delayWriter);

                // OK
                break;

              } catch (InterruptedException e) {
                System.out.println("Writer received interrupt (db=" + graph.getRawGraph().getURL());
                Thread.currentThread().interrupt();
                break;
              } catch (ORecordDuplicatedException e) {
                // IGNORE IT
              } catch (ONeedRetryException e) {
                System.out.println("Writer received exception (db=" + graph.getRawGraph().getURL());

                if (retry >= maxRetries)
                  e.printStackTrace();

                break;
              } catch (ODistributedException e) {
                if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                  graph.rollback();
                  throw e;
                }
              } catch (Throwable e) {
                System.out.println("Writer received exception (db=" + graph.getRawGraph().getURL());
                e.printStackTrace();
                return null;
              }
            }
          } finally {
            runningWriters.countDown();
          }
        }

        graph.getRawGraph().close();

        System.out.println("\nWriter " + name + " END");
      }catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    protected OrientVertex createVertex(OrientBaseGraph graph, int i) {

      final String uniqueId = shardName + "-s" + serverId + "-t" + threadId + "-v" + i;
      OrientVertex client = graph.addVertex("class:Client");
      client.setProperties("name", uniqueId, "updated", false);
      client.save();

      return client;
    }

    protected void updateVertex(OrientBaseGraph graph, int i) {
      OrientVertex vertex = loadVertex(graph, this.shardName, this.serverId, this.threadId, i);
      vertex.setProperty("updated", true);
      vertex.save();
    }

    protected void checkVertex(OrientBaseGraph graph, int i) {
      OrientVertex vertex = loadVertex(graph, this.shardName, this.serverId, this.threadId, i);
      junit.framework.Assert.assertEquals(vertex.getProperty("updated"), Boolean.TRUE);
    }

    protected void checkIndex(OrientBaseGraph graph, final String key, final ORID rid) {

      List<ODocument> result = graph.getRawGraph().query(new OSQLSynchQuery<OIdentifiable>("select from `index:Client.name` where key = ?"));

      assertNotNull(result);
      assertEquals(result.size(), 1);
      assertNotNull(result.get(0).getRecord());
      assertEquals((result.get(0)).field("rid"), rid);
    }

    protected void updateVertex(OrientBaseGraph graph, OrientVertex vertex) {
      vertex.setProperty("updated", true);
      vertex.save();
    }

    protected void checkVertex(OrientBaseGraph graph, OrientVertex vertex) {
      vertex.reload();
      junit.framework.Assert.assertEquals(vertex.getProperty("updated"), Boolean.TRUE);
    }

    protected void deleteRecord(OrientBaseGraph graph, OrientVertex vertex) {
      vertex.remove();
    }

    protected void checkRecordIsDeleted(OrientBaseGraph graph, OrientVertex vertex) {
      try {
        vertex.reload();
        junit.framework.Assert.fail("Record found while it should be deleted");
      } catch (ORecordNotFoundException e) {
      }
    }
  }

  @Override
  public String getDatabaseName() {
    return "sharding";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sct-sharded-basic-distrib-config-dserver" + server.getServerId() + ".xml";
  }

}
