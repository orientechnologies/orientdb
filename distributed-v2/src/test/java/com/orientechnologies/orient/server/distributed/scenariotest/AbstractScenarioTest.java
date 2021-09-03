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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterInsertTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

// import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;

/**
 * It represents an abstract scenario test.
 *
 * @author Gabriele Ponzi
 * @email <gabriele.ponzi--at--gmail.com>
 */
public abstract class AbstractScenarioTest extends AbstractServerClusterInsertTest {

  protected static final int SERVERS = 3;
  protected static final ODocument MISSING_DOCUMENT = new ODocument();

  // FIXME: these should be parameters read from configuration file (or, if missing, defaulted to
  // some values)
  private final long PROPAGATION_DOCUMENT_RETRIEVE_TIMEOUT = 15000;
  protected final long DOCUMENT_WRITE_TIMEOUT = 10000;

  protected ODocument loadRecord(ODatabaseDocument database, int serverId, int threadId, int i) {
    final String uniqueId = serverId + "-" + threadId + "-" + i;
    database.activateOnCurrentThread();
    List<ODocument> result =
        database.query(
            new OSQLSynchQuery<ODocument>(
                "select from Person where name = 'Billy" + uniqueId + "'"));
    if (result.size() == 0)
      assertTrue("No record found with name = 'Billy" + uniqueId + "'!", false);
    else if (result.size() > 1)
      assertTrue(result.size() + " records found with name = 'Billy" + uniqueId + "'!", false);
    //    ODatabaseRecordThreadLocal.instance().set(null);
    return result.get(0);
  }

  protected void executeMultipleWrites(List<ServerRun> executeOnServers, String storageType)
      throws InterruptedException, ExecutionException {
    executeMultipleWrites(executeOnServers, storageType, null, serverInstance);
  }

  protected void executeMultipleWrites(
      List<ServerRun> executeOnServers, String storageType, List<ServerRun> checkOnServers)
      throws InterruptedException, ExecutionException {
    executeMultipleWrites(executeOnServers, storageType, null, checkOnServers);
  }

  /*
   * It executes multiple writes using different concurrent writers (as specified by the value writerCount) on all the servers
   * present in the collection passed as parameter. Each write performs a document insert and some update and check operations on
   * it. Tha target db is passed as parameter, otherwise is kept the default one on servers.
   */

  protected void executeMultipleWrites(
      final List<ServerRun> executeOnServers,
      final String storageType,
      final String dbURL,
      final List<ServerRun> checkOnServers)
      throws InterruptedException, ExecutionException {

    ODatabaseDocument database;
    if (checkOnServers == null || checkOnServers.isEmpty()) {
      database = getDatabase();
    } else {
      database = getDatabase(checkOnServers.get(0));
    }

    try {
      List<ODocument> result =
          database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
      baseCount = ((Number) result.get(0).field("count")).intValue();
    } finally {
      if (database != null) database.close();
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
          if (storageType.equals("plocal")) {
            writer = createWriter(serverId, threadId++, server);
          } else if (storageType.equals("remote")) {
            writer = createWriter(serverId, threadId++, server);
          } else
            throw new IllegalArgumentException("storageType " + storageType + " not supported");
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
        Callable<Void> reader = createReader(server);
        readerWorkers.add(reader);
      }
    }

    List<Future<Void>> rFutures = readerExecutors.invokeAll(readerWorkers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    writerExecutors.shutdown();
    assertTrue(writerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All writer threads have finished, shutting down readers");

    for (Future<Void> future : rFutures) {
      future.get();
    }

    readerExecutors.shutdown();
    assertTrue(readerExecutors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : executeOnServers) {
      if (server.isActive()) {
        printStats(server);
      }
    }

    onBeforeChecks();

    checkInsertedEntries(checkOnServers);
    checkIndexedEntries(executeTestsOnServers);
  }

  // checks the consistency in the cluster after the writes in a simple distributed scenario
  protected void checkWritesAboveCluster(
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
      dbs.add(getDatabase(server));
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

    List<ODocument> docsToCompare = new LinkedList<ODocument>();

    super.banner(
        "Checking consistency among servers...\nChecking on servers {"
            + checkOnServer
            + "} that all the records written on {"
            + writtenServer
            + "} are consistent.");

    try {

      int index = 0;
      String serverName = null;

      for (int serverId : serverIndex2thresholdThread.keySet()) {

        serverName = serverIndex2serverName.get(serverId);
        System.out.println("Checking records originally inserted on server " + serverName + "...");

        // checking records inserted on server0
        int i;
        if (serverId == 0) i = 0;
        else i = serverIndex2thresholdThread.get(serverId - 1);

        while (i < serverIndex2thresholdThread.get(serverId)) {
          for (int j = 0; j < 100; j++) {

            // load records to compare
            for (ODatabaseDocument db : dbs) {
              docsToCompare.add(loadRecord(db, serverId, i, j + baseCount));
            }

            // checking that record is present on each server db
            for (ODocument doc : docsToCompare) {
              assertTrue(doc != null);
            }

            // checking that all the records have the same version and values (each record is equal
            // to the next one)
            int k = 0;
            while (k <= docsToCompare.size() - 2) {
              assertEquals(
                  "Inconsistency detected. Record: "
                      + docsToCompare.get(k).toString()
                      + " ; Servers: "
                      + (k + 1)
                      + ","
                      + (k + 2),
                  (Integer) docsToCompare.get(k).field("@version"),
                  (Integer) docsToCompare.get(k + 1).field("@version"));
              assertEquals(
                  "Inconsistency detected. Record: "
                      + docsToCompare.get(k).toString()
                      + " ; Servers: "
                      + (k + 1)
                      + ","
                      + (k + 2),
                  (String) docsToCompare.get(k).field("name"),
                  (String) docsToCompare.get(k + 1).field("name"));
              assertEquals(
                  "Inconsistency detected. Record: "
                      + docsToCompare.get(k).toString()
                      + " ; Servers: "
                      + (k + 1)
                      + ","
                      + (k + 2),
                  (String) docsToCompare.get(k).field("surname"),
                  (String) docsToCompare.get(k + 1).field("surname"));
              assertEquals(
                  "Inconsistency detected. Record: "
                      + docsToCompare.get(k).toString()
                      + " ; Servers: "
                      + (k + 1)
                      + ","
                      + (k + 2),
                  (Date) docsToCompare.get(k).field("birthday"),
                  (Date) docsToCompare.get(k + 1).field("birthday"));
              assertEquals(
                  "Inconsistency detected. Record: "
                      + docsToCompare.get(k).toString()
                      + " ; Servers: "
                      + (k + 1)
                      + ","
                      + (k + 2),
                  (String) docsToCompare.get(k).field("children"),
                  (String) docsToCompare.get(k + 1).field("children"));
              k++;
            }
            docsToCompare.clear();
          }
          i++;
        }

        System.out.println(
            "All records originally inserted on server "
                + serverName
                + " are consistent in the cluster.");
        index++;
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {

      for (ODatabaseDocument db : dbs) {
        db.activateOnCurrentThread();
        db.close();
        //        ODatabaseRecordThreadLocal.instance().set(db);
        //        db.close();
        //        ODatabaseRecordThreadLocal.instance().set(null);
      }
    }
  }

  // waiting for all the records' inserts in the cluster are propagated in the cluster inside a
  // specific timebox passed as timeout
  // parameter
  protected void waitForMultipleInsertsInClassPropagation(
      final long expectedCount, final String className, final long timeout) {

    waitFor(
        timeout,
        new OCallable<Boolean, Void>() {

          @Override
          public Boolean call(Void iArgument) {

            for (ServerRun server : serverInstance) {
              if (selectCountInClass(server, className) != expectedCount) {
                return false;
              }
            }
            return true;
          }
        },
        String.format("Expected %s records in class %s", expectedCount, className));
  }

  protected void waitForInsertedRecordPropagation(final String recordId) {
    waitForRecordPropagation(recordId, true);
  }

  protected void waitForDeletedRecordPropagation(final String recordId) {
    waitForRecordPropagation(recordId, false);
  }

  protected void waitForRecordPropagation(final String recordId, final boolean hasToBePresent) {

    waitFor(
        PROPAGATION_DOCUMENT_RETRIEVE_TIMEOUT,
        new OCallable<Boolean, Void>() {

          @Override
          public Boolean call(Void iArgument) {

            for (ServerRun server : serverInstance) {
              if ((retrieveRecordOrReturnMissing(server, recordId) == MISSING_DOCUMENT)
                  == hasToBePresent) {
                return false;
              }
            }
            return true;
          }
        },
        String.format(
            "Waiting for %s propagation of record %s",
            hasToBePresent ? "insert" : "delete", recordId));
  }

  protected void waitForUpdatedRecordPropagation(
      final String recordId, final String fieldName, final String expectedFieldValue) {

    waitFor(
        PROPAGATION_DOCUMENT_RETRIEVE_TIMEOUT,
        new OCallable<Boolean, Void>() {

          @Override
          public Boolean call(Void iArgument) {

            if (fieldName == null) return false;

            for (ServerRun server : serverInstance) {

              ODocument document = retrieveRecordOrReturnMissing(server, recordId);
              final String storedValue = document.field(fieldName);

              OLogManager.instance()
                  .debug(
                      this,
                      "Read record [%s] from server%s - %s: %s ",
                      recordId,
                      server.getServerId(),
                      fieldName,
                      storedValue);

              if (document == MISSING_DOCUMENT) {
                return false;
              }

              OLogManager.instance()
                  .info(
                      this,
                      "Waiting for updated document propagation on record %s. Found %s=%s, expected %s",
                      recordId,
                      fieldName,
                      storedValue,
                      expectedFieldValue);

              if (storedValue != null && !storedValue.equals(expectedFieldValue)
                  || storedValue == null && expectedFieldValue != null) return false;
            }
            return true;
          }
        },
        String.format(
            "Expected value %s for field %s on record %s on all servers.",
            expectedFieldValue, fieldName, recordId));
  }

  protected ODocument retrieveRecord(
      ServerRun serverRun,
      String uniqueId,
      boolean returnsMissingDocument,
      OCallable<ODocument, ODocument> assertion) {
    ODatabaseDocument dbServer = getDatabase(serverRun);
    // dbServer.getLocalCache().invalidate();
    //    ODatabaseRecordThreadLocal.instance().set(dbServer);

    dbServer.getMetadata().getSchema().reload();

    try {
      List<ODocument> result =
          dbServer.query(
              new OSQLSynchQuery<ODocument>("select from Person where id = '" + uniqueId + "'"));
      if (result.size() == 0) {
        if (returnsMissingDocument) {
          return MISSING_DOCUMENT;
        }
        assertTrue("No record found with id = '" + uniqueId + "'!", false);
      } else if (result.size() > 1) {
        fail(result.size() + " records found with id = '" + uniqueId + "'!");
      }

      ODocument doc = (ODocument) result.get(0);
      // try {
      // doc.reload();
      // } catch (ORecordNotFoundException e) {
      //// e.printStackTrace();
      // }

      if (assertion != null) assertion.call(doc);

      return doc;
    } finally {
      dbServer.close();
      //      ODatabaseRecordThreadLocal.instance().set(null);
    }
  }

  private long selectCountInClass(ServerRun serverRun, String className) {
    ODatabaseDocument dbServer = getDatabase(serverRun);
    //    ODatabaseRecordThreadLocal.instance().set(dbServer);
    long numberOfRecords = 0L;
    try {
      List<ODocument> result =
          dbServer.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from " + className));
      numberOfRecords = ((Number) result.get(0).field("count")).longValue();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      dbServer.close();
      //      ODatabaseRecordThreadLocal.instance().set(null);
    }

    return numberOfRecords;
  }

  protected ODocument retrieveRecordOrReturnMissing(ServerRun serverRun, String uniqueId) {
    return retrieveRecord(serverRun, uniqueId, true, null);
  }

  protected ODocument retrieveRecord(ServerRun serverRun, String uniqueId) {
    return retrieveRecord(serverRun, uniqueId, false, null);
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  protected String getPlocalDatabaseURL(final ServerRun server) {
    return "plocal:" + server.getDatabasePath(getDatabaseName());
  }

  protected String getPlocalDatabaseURL(final ServerRun server, String databaseName) {
    return "plocal:" + server.getDatabasePath(databaseName);
  }

  protected String getDatabaseURL(final ServerRun server, String storageType) {

    if (storageType.equals("plocal")) return this.getPlocalDatabaseURL(server);
    else if (storageType.equals("remote")) return this.getRemoteDatabaseURL(server);
    return null;
  }

  protected void simulateServerFault(ServerRun serverRun, String faultName) {

    if (faultName.equals("shutdown")) serverRun.terminateServer();
    else if (faultName.equals("net-fault")) {
      serverRun.crashServer();
    }
  }

  protected void startCountMonitorTask(final String iClassName) {
    new Timer(true)
        .schedule(
            new TimerTask() {
              @Override
              public void run() {
                try {
                  ODatabaseDocument db = getDatabase();
                  try {
                    totalVertices.set(db.countClass(iClassName));
                  } catch (Exception e) {
                    e.printStackTrace();
                  } finally {
                    db.close();
                  }
                } catch (Exception e) {
                  // IGNORE IT
                }
              }
            },
            1000,
            1000);
  }

  @Override
  protected String getDistributedServerConfiguration(final ServerRun server) {
    return "orientdb-dserver-config-" + server.getServerId() + ".xml";
  }

  private void printStats(final ServerRun serverRun) {
    final ODatabaseDocument database = getDatabase(serverRun);
    try {
      // database.reload();
      List<ODocument> result =
          database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));

      final String name = database.getURL();

      System.out.println(
          "\nReader "
              + name
              + "  sql count: "
              + result.get(0)
              + " counting class: "
              + database.countClass("Person")
              + " counting cluster: "
              + database.countClusterElements("Person"));

    } finally {
      database.close();
    }
  }

  protected void waitFor(final int serverId, OCallable<Boolean, ODatabaseDocument> condition) {
    final ODatabaseDocument db = getDatabase(serverInstance.get(serverId));
    try {

      while (true) {
        if (condition.call(db)) {
          break;
        }

        try {
          Thread.sleep(200);
        } catch (InterruptedException e) {
          // IGNORE IT
        }
      }

    } finally {
      if (!db.isClosed()) {
        db.activateOnCurrentThread();
        db.close();

        //        ODatabaseRecordThreadLocal.instance().set(db);
        //        db.close();
        //       ODatabaseRecordThreadLocal.instance().set(null);
      }
    }
  }

  public void executeFutures(final Collection<Future<Void>> futures) {
    try {
      for (Future f : futures) {
        f.get();
      }
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  /*
   * A simple client that updates a record
   */
  protected class RecordUpdater implements Callable<Void> {

    private ServerRun serverRun;
    private ODocument recordToUpdate;
    private Map<String, Object> fields;
    private boolean useTransaction;

    protected RecordUpdater(
        ServerRun serverRun,
        ODocument recordToUpdate,
        Map<String, Object> fields,
        boolean useTransaction) {
      this.serverRun = serverRun;
      this.recordToUpdate = recordToUpdate;
      this.fields = fields;
      this.useTransaction = useTransaction;
    }

    protected RecordUpdater(
        final ServerRun serverRun,
        final String rid,
        final Map<String, Object> fields,
        final boolean useTransaction) {
      this.serverRun = serverRun;
      this.useTransaction = useTransaction;
      this.recordToUpdate = retrieveRecord(serverRun, rid);
      this.fields = fields;
    }

    @Override
    public Void call() throws Exception {

      final ODatabaseDocument dbServer = getDatabase(serverRun);

      try {
        if (useTransaction) {
          dbServer.begin();
        }

        for (String fieldName : fields.keySet()) {
          this.recordToUpdate.field(fieldName, fields.get(fieldName));
        }
        this.recordToUpdate.save();

        if (useTransaction) {
          dbServer.commit();
        }
      } finally {
        if (dbServer != null) dbServer.close();
      }

      return null;
    }
  }

  /*
   * A simple client that deletes a record
   */
  protected class RecordDeleter implements Callable<Void> {

    private ServerRun serverRun;
    private ODocument recordToDelete;
    private boolean useTransaction;

    protected RecordDeleter(ServerRun serverRun, ODocument recordToDelete, boolean useTransaction) {
      this.serverRun = serverRun;
      this.recordToDelete = recordToDelete;
      this.useTransaction = useTransaction;
    }

    protected RecordDeleter(ServerRun serverRun, String rid, boolean useTransaction) {
      this.serverRun = serverRun;
      this.useTransaction = useTransaction;
      this.recordToDelete = retrieveRecord(serverRun, rid);
    }

    @Override
    public Void call() throws Exception {

      ODatabaseDocument dbServer = getDatabase(serverRun);

      try {
        if (useTransaction) {
          dbServer.begin();
        }

        this.recordToDelete.delete();
        this.recordToDelete.save();

        if (useTransaction) {
          dbServer.commit();
        }
      } finally {
        if (dbServer != null) dbServer.close();
      }

      return null;
    }
  }
}
