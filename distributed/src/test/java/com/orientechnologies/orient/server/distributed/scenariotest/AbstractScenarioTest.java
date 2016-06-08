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

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.server.distributed.AbstractServerClusterInsertTest;
import com.orientechnologies.orient.server.distributed.ServerRun;
import com.orientechnologies.orient.server.distributed.impl.ODistributedStorageEventListener;

import java.util.*;
import java.util.concurrent.*;

import static org.junit.Assert.*;

/**
 * It represents an abstract scenario test.
 *
 * @author Gabriele Ponzi
 * @email  <gabriele.ponzi--at--gmail.com>
 */

public abstract class AbstractScenarioTest extends AbstractServerClusterInsertTest {

  protected final static int       SERVERS                               = 3;
  protected List<ServerRun>        executeWritesOnServers                = new LinkedList<ServerRun>();
  protected final static ODocument MISSING_DOCUMENT                      = new ODocument();

  // FIXME: these should be parameters read from configuration file (or, if missing, defaulted to some values)
  private final long               PROPAGATION_DOCUMENT_RETRIEVE_TIMEOUT = 15000;
  protected final long             DOCUMENT_WRITE_TIMEOUT                = 10000;

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

  protected void executeMultipleWrites(List<ServerRun> executeOnServers, String storageType) throws InterruptedException, ExecutionException {
    executeMultipleWrites(executeOnServers, storageType, null);
  }

  /*
   * It executes multiple writes using different concurrent writers (as specified by the value writerCount) on all the servers
   * present in the collection passed as parameter. Each write performs a document insert and some update and check operations on
   * it. Tha target db is passed as parameter, otherwise is kept the default one on servers.
   */

  protected void executeMultipleWrites(List<ServerRun> executeOnServers, String storageType, String dbURL)
      throws InterruptedException, ExecutionException {

    ODatabaseDocumentTx database;
    if(dbURL == null) {
      database = poolFactory.get(getPlocalDatabaseURL(serverInstance.get(0)), "admin", "admin").acquire();
    }
    else {
      database = poolFactory.get(dbURL, "admin", "admin").acquire();
    }

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
          if (storageType.equals("plocal")) {
            writer = createWriter(serverId, threadId++, getPlocalDatabaseURL(server));
          } else if (storageType.equals("remote")) {
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
        printStats(getPlocalDatabaseURL(server));
      }
    }

    onBeforeChecks();

    checkInsertedEntries();
    checkIndexedEntries();
  }

  // checks the consistency in the cluster after the writes in a simple distributed scenario
  protected void checkWritesAboveCluster(List<ServerRun> checkConsistencyOnServers, List<ServerRun> writerServer) {

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

    List<ODatabaseDocumentTx> dbs = new LinkedList<ODatabaseDocumentTx>();

    for (ServerRun server : checkConsistencyOnServers) {
      dbs.add(poolFactory.get(getPlocalDatabaseURL(server), "admin", "admin").acquire());
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
      serverIndex2serverName.put(serverIndex, server.getServerInstance().getDistributedManager().getLocalNodeName());
      serverIndex++;
    }

    List<ODocument> docsToCompare = new LinkedList<ODocument>();

    super.banner("Checking consistency among servers...\nChecking on servers {" + checkOnServer
        + "} that all the records written on {" + writtenServer + "} are consistent.");

    try {

      int index = 0;
      String serverName = null;

      for (int serverId : serverIndex2thresholdThread.keySet()) {

        serverName = serverIndex2serverName.get(serverId);
        System.out.println("Checking records originally inserted on server " + serverName + "...");

        // checking records inserted on server0
        int i;
        if (serverId == 0)
          i = 0;
        else
          i = serverIndex2thresholdThread.get(serverId - 1);

        while (i < serverIndex2thresholdThread.get(serverId)) {
          for (int j = 0; j < 100; j++) {

            // load records to compare
            for (ODatabaseDocumentTx db : dbs) {
              docsToCompare.add(loadRecord(db, serverId, i, j + baseCount));
            }

            // checking that record is present on each server db
            for (ODocument doc : docsToCompare) {
              assertTrue(doc != null);
            }

            // checking that all the records have the same version and values (each record is equal to the next one)
            int k = 0;
            while (k <= docsToCompare.size() - 2) {
              assertEquals(
                  "Inconsistency detected. Record: " + docsToCompare.get(k).toString() + " ; Servers: " + (k + 1) + "," + (k + 2),
                  docsToCompare.get(k).field("@version"), docsToCompare.get(k + 1).field("@version"));
              assertEquals(
                  "Inconsistency detected. Record: " + docsToCompare.get(k).toString() + " ; Servers: " + (k + 1) + "," + (k + 2),
                  docsToCompare.get(k).field("name"), docsToCompare.get(k + 1).field("name"));
              assertEquals(
                  "Inconsistency detected. Record: " + docsToCompare.get(k).toString() + " ; Servers: " + (k + 1) + "," + (k + 2),
                  docsToCompare.get(k).field("surname"), docsToCompare.get(k + 1).field("surname"));
              assertEquals(
                  "Inconsistency detected. Record: " + docsToCompare.get(k).toString() + " ; Servers: " + (k + 1) + "," + (k + 2),
                  docsToCompare.get(k).field("birthday"), docsToCompare.get(k + 1).field("birthday"));
              assertEquals(
                  "Inconsistency detected. Record: " + docsToCompare.get(k).toString() + " ; Servers: " + (k + 1) + "," + (k + 2),
                  docsToCompare.get(k).field("children"), docsToCompare.get(k + 1).field("children"));
              k++;
            }
            docsToCompare.clear();
          }
          i++;
        }

        System.out.println("All records originally inserted on server " + serverName + " are consistent in the cluster.");
        index++;
      }

    } catch (Exception e) {
      e.printStackTrace();
    } finally {

      for (ODatabaseDocumentTx db : dbs) {
        ODatabaseRecordThreadLocal.INSTANCE.set(db);
        db.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
      }
    }

  }

  // waiting for all the records' inserts in the cluster are propagated in the cluster inside a specific timebox passed as timeout
  // parameter
  protected void waitForMultipleInsertsInClassPropagation(final long expectedCount, final String className, final long timeout) {

    waitFor(timeout, new OCallable<Boolean, Void>() {

      @Override
      public Boolean call(Void iArgument) {

        for (ServerRun server : serverInstance) {
          if (selectCountInClass(getDatabaseURL(server), className) != expectedCount) {
            return false;
          }
        }
        return true;
      }
    }, String.format("Expected %s records in class %s", expectedCount, className));

  }

  protected void waitForInsertedRecordPropagation(final String recordId) {
    waitForRecordPropagation(recordId, true);
  }

  protected void waitForDeletedRecordPropagation(final String recordId) {
    waitForRecordPropagation(recordId, false);
  }

  protected void waitForRecordPropagation(final String recordId, final boolean hasToBePresent) {

    waitFor(PROPAGATION_DOCUMENT_RETRIEVE_TIMEOUT, new OCallable<Boolean, Void>() {

      @Override
      public Boolean call(Void iArgument) {

        for (ServerRun server : serverInstance) {
          if ((retrieveRecordOrReturnMissing(getDatabaseURL(server), recordId) == MISSING_DOCUMENT) == hasToBePresent) {
            return false;
          }
        }
        return true;
      }
    }, String.format("Waiting for %s propagation of record %s", hasToBePresent ? "insert" : "delete", recordId));

  }

  protected void waitForUpdatedRecordPropagation(final String recordId, final String fieldName, final String expectedFieldValue) {

    waitFor(PROPAGATION_DOCUMENT_RETRIEVE_TIMEOUT, new OCallable<Boolean, Void>() {

      @Override
      public Boolean call(Void iArgument) {

        if (fieldName == null)
          return false;

        for (ServerRun server : serverInstance) {

          ODocument document = retrieveRecordOrReturnMissing(getDatabaseURL(server), recordId);
          OLogManager.instance().debug(this, "Readed record [%s] from server%s - %s: %s ", recordId, server.getServerId(), fieldName, document.field(fieldName));

          if (document == MISSING_DOCUMENT) {
            return false;
          }

          if (document.field(fieldName) != null && !document.field(fieldName).equals(expectedFieldValue)
              || document.field(fieldName) == null && expectedFieldValue != null)
            return false;

          OLogManager.instance().info(this, "Waiting for updated document propagation on record %s (%s=%s)...", recordId, fieldName,
              expectedFieldValue);
        }
        return true;
      }
    }, String.format("Expected value %s for field %s on record %s on all servers.", expectedFieldValue, fieldName, recordId));
  }

  protected ODocument retrieveRecord(String dbUrl, String uniqueId, boolean returnsMissingDocument) {
    ODatabaseDocumentTx dbServer = poolFactory.get(dbUrl, "admin", "admin").acquire();
    //dbServer.getLocalCache().invalidate();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
    try {
      List<ODocument> result = dbServer.query(new OSQLSynchQuery<ODocument>("select from Person where id = '" + uniqueId + "'"));
      if (result.size() == 0) {
        if (returnsMissingDocument) {
          return MISSING_DOCUMENT;
        }
        assertTrue("No record found with id = '" + uniqueId + "'!", false);
      } else if (result.size() > 1) {
        fail(result.size() + " records found with id = '" + uniqueId + "'!");
      }

      ODocument doc = (ODocument) result.get(0);
//      try {
//        doc.reload();
//      } catch (ORecordNotFoundException e) {
////        e.printStackTrace();
//      }
      return doc;
    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(null);
    }
  }

  private long selectCountInClass(String dbUrl, String className) {
    ODatabaseDocumentTx dbServer = poolFactory.get(dbUrl, "admin", "admin").acquire();
    ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
    long numberOfRecords = 0L;
    try {
      List<ODocument> result = dbServer.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from " + className));
      numberOfRecords = ((Number) result.get(0).field("count")).longValue();
    } catch (Exception e) {
      e.printStackTrace();
    } finally {
      ODatabaseRecordThreadLocal.INSTANCE.set(null);
    }

    return numberOfRecords;
  }

  protected ODocument retrieveRecordOrReturnMissing(String dbUrl, String uniqueId) {
    return retrieveRecord(dbUrl, uniqueId, true);
  }

  protected ODocument retrieveRecord(String dbUrl, String uniqueId) {
    return retrieveRecord(dbUrl, uniqueId, false);
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

  protected String getRemoteDatabaseURL(final ServerRun server) {
    return "remote:" + server.getBinaryProtocolAddress() + "/" + getDatabaseName();
  }

  protected String getDatabaseURL(final ServerRun server, String storageType) {

    if (storageType.equals("plocal"))
      return this.getPlocalDatabaseURL(server);
    else if (storageType.equals("remote"))
      return this.getRemoteDatabaseURL(server);
    return null;
  }

  protected void simulateServerFault(ServerRun serverRun, String faultName) {

    if (faultName.equals("shutdown"))
      serverRun.terminateServer();
    else if (faultName.equals("net-fault")) {
      serverRun.crashServer();
    }
  }

  protected void startCountMonitorTask(final String iClassName) {
    new Timer(true).schedule(new TimerTask() {
      @Override
      public void run() {
        try {
          ODatabaseDocumentTx db = new ODatabaseDocumentTx(getDatabaseURL(serverInstance.get(0)));
          db.open("admin", "admin");
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
    }, 1000, 1000);
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

    } finally {
      database.close();
    }

  }

  protected void waitFor(final int serverId, OCallable<Boolean, ODatabaseDocumentTx> condition) {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(getRemoteDatabaseURL(serverInstance.get(serverId))).open("admin",
        "admin");
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
        ODatabaseRecordThreadLocal.INSTANCE.set(db);
        db.close();
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
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

    private String              dbServerUrl;
    private ODocument           recordToUpdate;
    private Map<String, Object> fields;
    private boolean             useTransaction;

    protected RecordUpdater(String dbServerUrl, ODocument recordToUpdate, Map<String, Object> fields, boolean useTransaction) {
      this.dbServerUrl = dbServerUrl;
      this.recordToUpdate = recordToUpdate;
      this.fields = fields;
      this.useTransaction = useTransaction;
    }

    protected RecordUpdater(final String dbServerUrl, final String rid, final Map<String, Object> fields,
        final boolean useTransaction) {
      this.dbServerUrl = dbServerUrl;
      this.useTransaction = useTransaction;
      this.recordToUpdate = retrieveRecord(dbServerUrl, rid);
      this.fields = fields;
    }

    @Override
    public Void call() throws Exception {

      final ODatabaseDocumentTx dbServer = poolFactory.get(dbServerUrl, "admin", "admin").acquire();

      if (useTransaction) {
        dbServer.begin();
      }

      ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
      for (String fieldName : fields.keySet()) {
        this.recordToUpdate.field(fieldName, fields.get(fieldName));
      }
      this.recordToUpdate.save();

      if (useTransaction) {
        dbServer.commit();
      }

      return null;
    }
  }

  /*
   * A simple client that deletes a record
   */
  protected class RecordDeleter implements Callable<Void> {

    private String    dbServerUrl;
    private ODocument recordToDelete;
    private boolean   useTransaction;

    protected RecordDeleter(String dbServerUrl, ODocument recordToDelete, boolean useTransaction) {
      this.dbServerUrl = dbServerUrl;
      this.recordToDelete = recordToDelete;
      this.useTransaction = useTransaction;
    }

    protected RecordDeleter(String dbServerUrl, String rid, boolean useTransaction) {
      this.dbServerUrl = dbServerUrl;
      this.useTransaction = useTransaction;
      this.recordToDelete = retrieveRecord(dbServerUrl, rid);
    }

    @Override
    public Void call() throws Exception {

      ODatabaseDocumentTx dbServer = poolFactory.get(dbServerUrl, "admin", "admin").acquire();

      if (useTransaction) {
        dbServer.begin();
      }

      ODatabaseRecordThreadLocal.INSTANCE.set(dbServer);
      this.recordToDelete.delete();
      this.recordToDelete.save();

      if (useTransaction) {
        dbServer.commit();
      }

      return null;
    }
  }

  class AfterRecordLockDelayer implements ODistributedStorageEventListener {

    private String serverName;
    private long   delay;

    public AfterRecordLockDelayer(String serverName, long delay) {
      this.serverName = serverName;
      this.delay = delay;
      OLogManager.instance().info(this, "Thread [%s-%d] delayer created with " + delay + "ms of delay", serverName,
          Thread.currentThread().getId());
    }

    public AfterRecordLockDelayer(String serverName) {
      this.serverName = serverName;
      this.delay = DOCUMENT_WRITE_TIMEOUT;
      OLogManager.instance().info(this, "Thread [%s-%d] delayer created with " + delay + "ms of delay", serverName,
          Thread.currentThread().getId());
    }

    @Override
    public void onAfterRecordLock(ORecordId rid) {
      if (delay > 0)
        try {
          OLogManager.instance().info(this, "Thread [%s-%d] waiting for %dms with locked record [%s]", serverName,
              Thread.currentThread().getId(), delay, rid.toString());
          Thread.sleep(delay);

          OLogManager.instance().info(this, "Thread [%s-%d] finished waiting for %dms with locked record [%s]", serverName,
              Thread.currentThread().getId(), delay, rid.toString());

          // RESET THE DELAY FOR FURTHER TIMES
          delay = 0;

        } catch (InterruptedException e) {
        }
    }

    @Override
    public void onAfterRecordUnlock(ORecordId rid) {
    }
  }

}
