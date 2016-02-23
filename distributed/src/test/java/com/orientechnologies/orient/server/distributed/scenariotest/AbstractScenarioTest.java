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

import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OQueryParsingException;
import com.orientechnologies.orient.core.metadata.OMetadataInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterInsertTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.hazelcast.OHazelcastPlugin;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * It represents an abstract scenario test.
 */

public abstract class AbstractScenarioTest extends AbstractServerClusterInsertTest {

  protected final static int SERVERS = 3;
  protected List<ServerRun> executeWritesOnServers = new LinkedList<ServerRun>();

  protected ODocument loadRecord(ODatabaseDocumentTx database, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;
    ODatabaseRecordThreadLocal.INSTANCE.set(database);
    List<ODocument> result = database
        .query(new OSQLSynchQuery<ODocument>("select from Person where name = 'Billy" + uniqueId + "'"));
    if (result.size() == 0)
      assertTrue("No record found with name = 'Billy" + uniqueId + "'!", false);
    else if (result.size() > 1)
      assertTrue(result.size() + " records found with name = 'Billy" + uniqueId + "'!", false);
    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    return result.get(0);
  }


    /*
     * It executes multiple writes using different concurrent writers (as specified by the value writerCount)
     * on all the servers present in the collection passed as parameter.
     * Each write performs a document insert and some update and check operations on it.
     */

  protected void executeMultipleWrites(List<ServerRun> executeOnServers, String storageType) throws InterruptedException, ExecutionException {

    ODatabaseDocumentTx database = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();

    try {
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
      baseCount = ((Number) result.get(0).field("count")).intValue();
    } finally {
      database.close();
    }

    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService writerExecutors = Executors.newCachedThreadPool();
    final ExecutorService readerExecutors = Executors.newCachedThreadPool();

    runningWriters = new CountDownLatch(executeOnServers.size() * writerCount);

    int serverId = 0;
    int threadId = 0;
    List<Callable<Void>> writerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : executeOnServers) {
      if (server.isActive()) {
        for (int j = 0; j < writerCount; j++) {
          Callable writer = null;
          if(storageType.equals("plocal")) {
            writer = createWriter(serverId, threadId++, getPlocalDatabaseURL(server));
          }
          else if(storageType.equals("remote")) {
            writer = createWriter(serverId, threadId++, getRemoteDatabaseURL(server));
          }
          writerWorkers.add(writer);
        }
        serverId++;
      }
    }

    expected = writerCount * count * serverId + baseCount;

    System.out.println("Writes started.");
    List<Future<Void>> futures = writerExecutors.invokeAll(writerWorkers);

    List<Callable<Void>> readerWorkers = new ArrayList<Callable<Void>>();
    for (ServerRun server : executeOnServers) {
      if (server.isActive()) {
        Callable<Void> reader = createReader(getPlocalDatabaseURL(server));
        readerWorkers.add(reader);
      }
    }

    List<Future<Void>> rFutures = readerExecutors.invokeAll(readerWorkers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    writerExecutors.shutdown();
    junit.framework.Assert.assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished, shutting down readers");

    for (Future<Void> future : rFutures) {
      future.get();
    }

    readerExecutors.shutdown();
    junit.framework.Assert.assertTrue(readerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : executeOnServers) {
      if (server.isActive()) {
        printStats(getPlocalDatabaseURL(server));
      }
    }

    checkInsertedEntries();
    checkIndexedEntries();
  }


  // checks the consistency in the cluster after the writes in a simple distributed scenario
  protected void checkWritesAboveCluster(List<ServerRun> checkConsistencyOnServers, List<ServerRun> writerServer) {

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

    List<ODatabaseDocumentTx> dbs = new LinkedList<ODatabaseDocumentTx>();

    for(ServerRun server: checkConsistencyOnServers) {
      dbs.add(poolFactory.get(getPlocalDatabaseURL(server), "admin", "admin").acquire());
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

    List<ODocument> docsToCompare = new LinkedList<ODocument>();

    super.banner("Checking consistency among servers...\nChecking on servers {" + checkOnServer + "} that all the records written on {" + writtenServer + "} are consistent.");

    try {

      int index = 0;
      String serverName = null;

      for(int serverId: serverIndex2thresholdThread.keySet()) {

        serverName = serverIndex2serverName.get(serverId);
        System.out.println("Checking records originally inserted on server " + serverName + "...");

        // checking records inserted on server0
        int i;
        if(serverId == 0)
          i = 0;
        else
          i = serverIndex2thresholdThread.get(serverId-1);

        while (i < serverIndex2thresholdThread.get(serverId)) {
          for (int j = 0; j < 100; j++) {

            // load records to compare
            for(ODatabaseDocumentTx db: dbs) {
              docsToCompare.add(loadRecord(db, serverId, i, j + baseCount));
            }

            // checking that record is present on each server db
            for(ODocument doc: docsToCompare) {
              assertTrue(doc != null);
            }

            // checking that all the records have the same version and values (each record is equal to the next one)
            int k = 0;
            while(k <= docsToCompare.size() -2) {
              assertEquals(docsToCompare.get(k).field("@version"), docsToCompare.get(k+1).field("@version"));
              assertEquals(docsToCompare.get(k).field("name"), docsToCompare.get(k+1).field("name"));
              assertEquals(docsToCompare.get(k).field("surname"), docsToCompare.get(k+1).field("surname"));
              assertEquals(docsToCompare.get(k).field("birthday"), docsToCompare.get(k+1).field("birthday"));
              assertEquals(docsToCompare.get(k).field("children"), docsToCompare.get(k+1).field("children"));
              k++;
            }
            docsToCompare.clear();
          }
          i++;
        }

        System.out.println("All records originally inserted on server " + serverName + " are consistent in the cluster.");
        index++;
      }

    } catch(Exception e) {
      e.printStackTrace();
    } finally {

      for(ODatabaseDocumentTx db: dbs) {
        ODatabaseRecordThreadLocal.INSTANCE.set(db);
        db.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }

  }

  protected ODocument retrieveRecord(String dbUrl, String uniqueId) {
    ODatabaseDocumentTx dbServer = poolFactory.get(dbUrl, "admin", "admin").acquire();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
    List<ODocument> result = dbServer.query(new OSQLSynchQuery<ODocument>("select from Person where id = '" + uniqueId + "'"));
    if (result.size() == 0)
      assertTrue("No record found with id = '" + uniqueId + "'!", false);
    else if (result.size() > 1)
      assertTrue(result.size() + " records found with id = '" + uniqueId + "'!", false);
    ODatabaseRecordThreadLocal.INSTANCE.set(null);
    return result.get(0);
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  protected String getPlocalDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  protected String getRemoteDatabaseURL(final ServerRun server) {
    return "remote:localhost/" + this.getDatabaseName();
  }

  protected String getDatabaseURL(final ServerRun server, String storageType) {

    if(storageType.equals("plocal"))
      return this.getPlocalDatabaseURL(server);
    else if(storageType.equals("remote"))
      return this.getRemoteDatabaseURL(server);
    return null;
  }

  protected void simulateServerFault(ServerRun serverRun, String faultName) {

    OServer server = serverRun.getServerInstance();

    if(faultName.equals("shutdown"))
      serverRun.shutdownServer();
    else if(faultName.equals("net-fault")) {
      serverRun.crashServer();
    }
  }

  @Override
  public String getDatabaseName() {
    return "distributed-inserttxha";
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "orientdb-dserver-config-" + server.getServerId() + ".xml";
  }


  private void printStats(final String databaseUrl) {
    final ODatabaseDocumentTx database = poolFactory.get(databaseUrl, "admin", "admin").acquire();
    try {
      database.reload();
      List<ODocument> result = database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));

      final String name = database.getURL();

      System.out.println("\nReader " + name + " sql count: " + result.get(0) + " counting class: " + database.countClass("Person")
          + " counting cluster: " + database.countClusterElements("Person"));

      if (((OMetadataInternal) database.getMetadata()).getImmutableSchemaSnapshot().existsClass("ODistributedConflict"))
        try {
          List<ODocument> conflicts = database
              .query(new OSQLSynchQuery<OIdentifiable>("select count(*) from ODistributedConflict"));
          long totalConflicts = conflicts.get(0).field("count");
          junit.framework.Assert.assertEquals(0l, totalConflicts);
          System.out.println("\nReader " + name + " conflicts: " + totalConflicts);
        } catch (OQueryParsingException e) {
          // IGNORE IT
        }

    } finally {
      database.close();
    }

  }

}
