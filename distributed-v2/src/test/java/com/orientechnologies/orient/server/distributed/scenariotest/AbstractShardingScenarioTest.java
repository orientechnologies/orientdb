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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.sql.query.OLegacyResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.server.distributed.ODistributedException;
import com.orientechnologies.orient.server.distributed.ServerRun;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.Assert;

/**
 * It represents an abstract scenario test with sharding on the cluster.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public class AbstractShardingScenarioTest extends AbstractScenarioTest {

  protected OVertex loadVertex(
      ODatabaseDocument graph, String shardName, int serverId, int threadId, int i) {

    List<OVertex> result = null;

    try {
      final String uniqueId = shardName + "-s" + serverId + "-t" + threadId + "-v" + i;
      Iterable<OElement> it =
          graph
              .command(new OCommandSQL("select from Client where name = '" + uniqueId + "'"))
              .execute();
      result = new LinkedList<OVertex>();

      for (OElement v : it) {
        if (v.isVertex()) {
          result.add(v.asVertex().get());
        }
      }

      if (result.size() == 0) fail("No record found with name = '" + uniqueId + "'!");
      else if (result.size() > 1)
        fail(result.size() + " records found with name = '" + uniqueId + "'!");

      if (result.size() > 0) return result.get(0);

    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail("Error in loadVertex(): " + e.toString());
    }

    return null;
  }

  /*
   * It executes multiple writes using different concurrent writers (as specified by the value writerCount) on all the servers
   * present in the collection passed as parameter in a specific cluster-shards. Each write performs a vertex insert and some update
   * and check operations on it. Vertex name: <shardName>-s<serverId>-t<threadId>-<recordId>
   */

  protected void executeMultipleWritesOnShards(List<ServerRun> executeOnServers, String storageType)
      throws InterruptedException, ExecutionException {

    System.out.println("Creating Writers threads...");

    final ExecutorService writerExecutors = Executors.newCachedThreadPool();

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
          if (storageType.equals("plocal")) {
            writer = new ShardWriter(serverId, shardName, threadId++, getPlocalDatabaseURL(server));
          } else if (storageType.equals("remote")) {
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
    assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished.");

    OrientDB orientDB = serverInstance.get(0).getServerInstance().getContext();
    // checking inserted vertice
    // checking total amount of records (map-reduce aggregation)
    if (!orientDB.exists(getDatabaseName())) {
      orientDB.create(getDatabaseName(), ODatabaseType.PLOCAL);
    }
    ODatabaseDocument graph = orientDB.open(getDatabaseName(), "admin", "admin");

    try {
      OLegacyResultSet<ODocument> clients = new OCommandSQL("select from Client").execute();
      int total = clients.size();
      assertEquals(expected, total);

      List<ODocument> result = new OCommandSQL("select count(*) from Client").execute();
      total = ((Number) result.get(0).field("count")).intValue();
      // assertEquals(expected, total);
    } finally {
      graph.close();
    }

    serverId = 0;
    for (ServerRun server : serverInstance) {
      if (server.isActive()) {
        OrientDB orientDB1 = server.getServerInstance().getContext();
        if (!orientDB1.exists(getDatabaseName())) {
          orientDB1.create(getDatabaseName(), ODatabaseType.PLOCAL);
        }
        graph = orientDB1.open(getDatabaseName(), "admin", "admin");
        try {
          String sqlCommand =
              "select from cluster:client_"
                  + server.getServerInstance().getDistributedManager().getLocalNodeName();
          List<ODocument> result = new OCommandSQL(sqlCommand).execute();
          int total = result.size();
          // assertEquals(count * writerCount, total);

          sqlCommand =
              "select count(*) from cluster:client_"
                  + server.getServerInstance().getDistributedManager().getLocalNodeName();
          result = new OCommandSQL(sqlCommand).execute();
          total = ((Number) result.get(0).field("count")).intValue();
          // assertEquals(count * writerCount, total);
        } catch (Exception e) {
          e.printStackTrace();
        } finally {
          graph.close();
        }
      }
      serverId++;
    }
  }

  // checks the consistency in the cluster after the writes in a no-replica sharding scenario
  protected void checkAvailabilityOnShardsNoReplica(
      List<ServerRun> checkConsistencyOnServers, List<ServerRun> writerServer) {

    String checkOnServer = "";
    for (ServerRun server : checkConsistencyOnServers) {
      checkOnServer += server.getServerInstance().getDistributedManager().getLocalNodeName() + ",";
    }
    checkOnServer = checkOnServer.substring(0, checkOnServer.length() - 1);

    String writtenServer = "";
    for (ServerRun server : writerServer) {
      writtenServer += server.getServerInstance().getDistributedManager().getLocalNodeName() + ",";
    }
    writtenServer = writtenServer.substring(0, writtenServer.length() - 1);

    List<ODatabaseDocument> dbs = new LinkedList<ODatabaseDocument>();

    for (ServerRun server : checkConsistencyOnServers) {

      OrientDB context = server.getServerInstance().getContext();
      if (!context.exists(getDatabaseName())) {
        context.create(getDatabaseName(), ODatabaseType.PLOCAL);
      }
      ODatabaseSession db = context.open(getDatabaseName(), "admin", "admin");
      dbs.add(db);
    }

    Map<Integer, Integer> serverIndex2thresholdThread = new LinkedHashMap<Integer, Integer>();
    Map<Integer, String> serverIndex2serverName = new LinkedHashMap<Integer, String>();

    int lastThread = 0;
    int serverIndex = 0;

    for (ServerRun server : writerServer) {
      serverIndex2thresholdThread.put(serverIndex, lastThread + 5);
      serverIndex++;
      lastThread += 5;
    }

    serverIndex = 0;

    for (ServerRun server : writerServer) {
      serverIndex2serverName.put(
          serverIndex, server.getServerInstance().getDistributedManager().getLocalNodeName());
      serverIndex++;
    }

    List<OVertex> verticesToCheck = new LinkedList<OVertex>();

    super.banner(
        "Checking consistency among servers...\nChecking on servers {"
            + checkOnServer
            + "} that all the vertices written on {"
            + writtenServer
            + "} are consistent.");

    try {

      int index = 0;
      String serverName = null;

      for (int serverId : serverIndex2thresholdThread.keySet()) {

        serverName = serverIndex2serverName.get(serverId);
        System.out.println("Checking records originally inserted on server " + serverName + "...");

        String clusterName = "client_" + serverName;

        // checking records inserted on server0
        int i;
        if (serverId == 0) i = 0;
        else i = serverIndex2thresholdThread.get(serverId - 1);

        while (i < serverIndex2thresholdThread.get(serverId)) {
          for (int j = 0; j < 100; j++) {

            // load records to compare
            for (ODatabaseDocument db : dbs) {
              db.activateOnCurrentThread();
              verticesToCheck.add(loadVertex(db, clusterName, serverId, i, j + baseCount));
            }

            // checking that record is present on each server db
            OVertex currentVertex = null;
            int k = 0;
            while (k < verticesToCheck.size()) {
              assertTrue(verticesToCheck.get(k) != null);
              k++;
            }

            // checking that all the records have the same version and values (each record is equal
            // to the next one)
            k = 0;
            while (k <= verticesToCheck.size() - 2) {
              assertEquals(
                  verticesToCheck.get(k).getProperty("@version"),
                  verticesToCheck.get(k + 1).getProperty("@version"));
              assertEquals(
                  verticesToCheck.get(k).getProperty("name"),
                  verticesToCheck.get(k + 1).getProperty("name"));
              assertEquals(
                  verticesToCheck.get(k).getProperty("updated"),
                  verticesToCheck.get(k + 1).getProperty("updated"));
              k++;
            }
            verticesToCheck.clear();
          }
          i++;
        }

        System.out.println(
            "All records originally inserted on server "
                + serverName
                + " in the cluster "
                + clusterName
                + " available in the shard.");
        index++;
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {

      for (ODatabaseDocument db : dbs) {
        db.activateOnCurrentThread();
        db.close();
        ODatabaseRecordThreadLocal.instance().set(null);
      }
    }
  }

  /*
   * A Callable task that inserts many vertices as indicated by the count variable on the specified server and cluster (shard).
   */

  protected class ShardWriter implements Callable<Void> {
    protected final String databaseUrl;
    protected int serverId;
    protected final String shardName;
    protected int threadId;

    protected ShardWriter(
        final int iServerId, final String shardName, final int iThreadId, final String db) {
      serverId = iServerId;
      this.shardName = shardName;
      threadId = iThreadId;
      databaseUrl = db;
    }

    @Override
    public Void call() throws Exception {

      try {

        String name = Integer.toString(threadId);
        OrientDB orientDB = serverInstance.get(serverId).getServerInstance().getContext();
        // checking inserted vertice
        // checking total amount of records (map-reduce aggregation)
        if (!orientDB.exists(getDatabaseName())) {
          orientDB.create(getDatabaseName(), ODatabaseType.PLOCAL);
        }
        ODatabaseDocument graph = orientDB.open(getDatabaseName(), "admin", "admin");

        for (int i = 0; i < count; i++) {

          try {
            final int id = baseCount + i;

            int retry = 0;

            for (retry = 0; retry < maxRetries; retry++) {

              try {
                final OVertex client = createVertex(graph, id);

                updateVertex(graph, id);
                checkVertex(graph, id);

                if ((i + 1) % 100 == 0)
                  System.out.println(
                      "\nDBStartupWriter "
                          + graph.getURL()
                          + " managed "
                          + (i + 1)
                          + "/"
                          + count
                          + " records so far");

                if (delayWriter > 0) Thread.sleep(delayWriter);

                // OK
                break;

              } catch (InterruptedException e) {
                System.out.println("DBStartupWriter received interrupt (db=" + graph.getURL());
                Thread.currentThread().interrupt();
                break;
              } catch (ORecordDuplicatedException e) {
                // IGNORE IT
              } catch (ONeedRetryException e) {
                System.out.println("DBStartupWriter received exception (db=" + graph.getURL());

                if (retry >= maxRetries) e.printStackTrace();

                break;
              } catch (ODistributedException e) {
                if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                  graph.rollback();
                  throw e;
                }
              } catch (Throwable e) {
                System.out.println("DBStartupWriter received exception (db=" + graph.getURL());
                e.printStackTrace();
                return null;
              }
            }
          } finally {
            runningWriters.countDown();
          }
        }

        graph.close();

        System.out.println("\nDBStartupWriter " + name + " END");
      } catch (Exception e) {
        e.printStackTrace();
      }
      return null;
    }

    protected OVertex createVertex(ODatabaseDocument graph, int i) {

      final String uniqueId = shardName + "-s" + serverId + "-t" + threadId + "-v" + i;

      OVertex client = graph.newVertex("Client");
      client.setProperty("name", uniqueId);
      client.setProperty("updated", false);
      client.save(shardName);

      return client;
    }

    protected void updateVertex(ODatabaseDocument graph, int i) {
      OVertex vertex = loadVertex(graph, this.shardName, this.serverId, this.threadId, i);
      vertex.setProperty("updated", true);
      vertex.save();
    }

    protected void checkVertex(ODatabaseDocument graph, int i) {
      OVertex vertex = loadVertex(graph, this.shardName, this.serverId, this.threadId, i);
      assertEquals(vertex.getProperty("updated"), Boolean.TRUE);
    }

    protected void updateVertex(ODatabaseDocument graph, OVertex vertex) {
      vertex.setProperty("updated", true);
      vertex.save();
    }

    protected void checkVertex(ODatabaseDocument graph, OVertex vertex) {
      vertex.reload();
      assertEquals(vertex.getProperty("updated"), Boolean.TRUE);
    }

    protected void deleteRecord(ODatabaseDocument graph, OVertex vertex) {
      vertex.delete();
    }

    protected void checkRecordIsDeleted(ODatabaseDocument graph, OVertex vertex) {
      try {
        vertex.reload();
        fail("Record found while it should be deleted");
      } catch (ORecordNotFoundException e) {
      }
    }
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "sct-sharded-basic-distrib-config-dserver" + server.getServerId() + ".xml";
  }
}
