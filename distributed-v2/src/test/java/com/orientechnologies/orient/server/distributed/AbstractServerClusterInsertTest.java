/*
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.server.distributed;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import com.orientechnologies.orient.distributed.impl.metadata.OClassDistributed;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Assert;

/** Insert records concurrently against the cluster */
public abstract class AbstractServerClusterInsertTest extends AbstractDistributedWriteTest {
  protected volatile int delayWriter = 0;
  protected volatile int delayReader = 1000;
  protected static int writerCount = 5;
  protected int baseCount = 0;
  protected long expected;
  protected OIndex idx;
  protected int maxRetries = 5;
  protected boolean useTransactions = false;
  protected List<ServerRun> executeTestsOnServers = null;
  protected String className = "Person";
  protected String indexName = "Person.name";

  protected class BaseWriter implements Callable<Void> {
    protected final ServerRun serverRun;
    protected final int serverId;
    protected final int threadId;

    protected BaseWriter(final int iServerId, final int iThreadId, final ServerRun serverRun) {
      serverId = iServerId;
      threadId = iThreadId;
      this.serverRun = serverRun;
    }

    @Override
    public Void call() throws Exception {
      int j = 0;
      String name = Integer.toString(threadId);

      for (int i = 0; i < count; i++) {
        final ODatabaseDocument database = getDatabase(serverRun);

        try {
          final int id = baseCount + i;

          final String uid = UUID.randomUUID().toString();

          int retry;
          for (retry = 0; retry < maxRetries; retry++) {
            if (useTransactions) database.begin();

            try {
              final ODocument person = createRecord(database, id, uid);

              if (!useTransactions) {
                updateRecord(database, id);
                checkRecord(database, id);
                checkIndex(database, (String) person.field("name"), person.getIdentity());
              }

              if (useTransactions) database.commit();

              if ((i + 1) % 100 == 0)
                System.out.println(
                    "\nWriter "
                        + database.getURL()
                        + " managed "
                        + (i + 1)
                        + "/"
                        + count
                        + " records so far");

              if (delayWriter > 0) Thread.sleep(delayWriter);

              // OK
              break;

            } catch (InterruptedException e) {
              System.out.println("Writer received interrupt (db=" + database.getURL());
              Thread.currentThread().interrupt();
              break;
            } catch (ORecordDuplicatedException e) {
              System.out.println("Writer received exception (db=" + database.getURL());
              // IGNORE IT
            } catch (ONeedRetryException e) {
              System.out.println("Writer received exception (db=" + database.getURL());

              if (retry >= maxRetries) e.printStackTrace();

            } catch (ODistributedException e) {
              if (!(e.getCause() instanceof ORecordDuplicatedException)) {
                database.rollback();
                throw e;
              }
            } catch (Throwable e) {
              System.out.println("Writer received exception (db=" + database.getURL() + ")");
              e.printStackTrace();
              return null;
            }
          }
        } finally {
          runningWriters.countDown();
          database.activateOnCurrentThread();
          database.close();
        }
        j++;
      }

      System.out.println("\nWriter " + name + " END");
      return null;
    }

    protected ODocument createRecord(ODatabaseDocument database, int i, final String uid)
        throws InterruptedException {
      checkClusterStrategy(database);

      final String uniqueId = serverId + "-" + threadId + "-" + i;

      ODocument person = null;
      for (int retry = 0; retry < 10; ++retry) {
        person =
            new ODocument("Person")
                .fields(
                    "id",
                    uid,
                    "name",
                    "Billy" + uniqueId,
                    "surname",
                    "Mayes" + uniqueId,
                    "birthday",
                    new Date(),
                    "children",
                    uniqueId);
        try {
          database.save(person);
          break;
        } catch (ONeedRetryException e) {
          // RETRY
          System.out.println("EXCEPTION " + e.getCause() + " RETRY " + retry + " ON CREATE RECORD");
          Thread.sleep(200);

        } catch (OException e) {
          if (e.getCause() instanceof ONeedRetryException) {
            // RETRY
            System.out.println(
                "EXCEPTION " + e.getCause().getCause() + " RETRY " + retry + " ON CREATE RECORD");
            Thread.sleep(200);
          } else throw e;
        }
      }

      if (!useTransactions) Assert.assertTrue(person.getIdentity().isPersistent());

      return person;
    }

    protected void updateRecord(ODatabaseDocument database, int i) {
      checkClusterStrategy(database);

      ODocument doc = loadRecord(database, i);
      doc.field("updated", true);
      doc.save();
    }

    protected void checkRecord(ODatabaseDocument database, int i) {
      checkClusterStrategy(database);
      ODocument doc = loadRecord(database, i);
      Assert.assertEquals(doc.field("updated"), Boolean.TRUE);
    }

    protected void checkIndex(ODatabaseDocument database, final String key, final ORID rid) {
      checkClusterStrategy(database);

      final OIndex index =
          ((ODatabaseDocumentInternal) database)
              .getSharedContext()
              .getIndexManager()
              .getIndex((ODatabaseDocumentInternal) database, indexName);
      final List<ORID> rids;
      try (Stream<ORID> stream = index.getInternal().getRids(key)) {
        rids = stream.collect(Collectors.toList());
      }
      Assert.assertEquals(1, rids.size());
      Assert.assertTrue(rids.contains(rid));
    }

    protected ODocument loadRecord(ODatabaseDocument database, int i) {
      checkClusterStrategy(database);

      final String uniqueId = serverId + "-" + threadId + "-" + i;

      List<ODocument> result =
          database.query(
              new OSQLSynchQuery<ODocument>("select from Person where name = ?"),
              "Billy" + uniqueId);
      if (result.size() == 0)
        Assert.assertTrue("No record found with name = 'Billy" + uniqueId + "'!", false);
      else if (result.size() > 1)
        Assert.assertTrue(
            result.size() + " records found with name = 'Billy" + uniqueId + "'!", false);

      return result.get(0);
    }

    protected void updateRecord(ODatabaseDocument database, ODocument doc) {
      checkClusterStrategy(database);

      doc.field("updated", true);
      doc.save();
    }

    protected void checkRecord(ODatabaseDocument database, ODocument doc) {
      checkClusterStrategy(database);

      doc.reload();
      Assert.assertEquals(doc.field("updated"), Boolean.TRUE);
    }

    private void checkClusterStrategy(ODatabaseDocument database) {
      if (!database.getURL().startsWith("remote:"))
        Assert.assertTrue(
            database.getMetadata().getSchema().getClass("Person") instanceof OClassDistributed);
    }

    protected void deleteRecord(ODatabaseDocument database, ODocument doc) {
      checkClusterStrategy(database);

      doc.delete();
    }

    protected void checkRecordIsDeleted(ODatabaseDocument database, ODocument doc) {
      checkClusterStrategy(database);

      try {
        doc.reload();
        Assert.fail("Record found while it should be deleted");
      } catch (ORecordNotFoundException e) {
      }
    }
  }

  class Reader implements Callable<Void> {
    private final ServerRun serverRun;

    public Reader(final ServerRun serverRun) {
      this.serverRun = serverRun;
    }

    @Override
    public Void call() throws Exception {
      try {
        while (runningWriters.getCount() > 0) {
          try {
            printStats(serverRun);

            if (delayReader > 0) Thread.sleep(delayReader);

          } catch (Exception e) {
            break;
          }
        }

      } finally {
        printStats(serverRun);
      }
      return null;
    }
  }

  @Override
  public void executeTest() throws Exception {

    ODatabaseDocument database = getDatabase();

    try {
      new ODocument("Customer").fields("name", "Jay", "surname", "Miner").save();
      new ODocument("Customer").fields("name", "Luke", "surname", "Skywalker").save();
      new ODocument("Provider").fields("name", "Yoda", "surname", "Nothing").save();
    } finally {
      database.close();
    }

    executeMultipleTest();
    dropIndexNode1();

    recreateIndexNode2();
  }

  protected void executeMultipleTest()
      throws InterruptedException, java.util.concurrent.ExecutionException {
    executeMultipleTest(0);
  }

  @Override
  protected void prepare(boolean iCopyDatabaseToNodes, boolean iCreateDatabase) throws Exception {
    super.prepare(iCopyDatabaseToNodes, iCreateDatabase);

    executeTestsOnServers = new ArrayList<ServerRun>(serverInstance);
  }

  protected void executeMultipleTest(final int serverNum)
      throws InterruptedException, java.util.concurrent.ExecutionException {
    ODatabaseDocument database =
        getDatabase(
            serverNum); // serverInstance.get(serverNum).getEmbeddedDatabase(getDatabaseName());

    try {
      List<ODocument> result =
          database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from Person"));
      baseCount = ((Number) result.get(0).field("count")).intValue();
    } finally {
      database.close();
    }

    System.out.println("Creating Writers and Readers threads...");

    final ExecutorService executors = Executors.newCachedThreadPool();

    runningWriters = new CountDownLatch(executeTestsOnServers.size() * writerCount);

    int serverId = 0;
    int threadId = 0;

    List<Callable<Void>> workers = new ArrayList<Callable<Void>>();
    for (ServerRun server : executeTestsOnServers) {
      if (server.isActive()) {
        for (int j = 0; j < writerCount; j++) {
          Callable writer = createWriter(serverId, threadId++, server);
          workers.add(writer);
        }

        Callable<Void> reader = createReader(server);
        workers.add(reader);

        serverId++;
      }
    }

    computeExpected(serverId);

    System.out.println("Expected records=" + expected);

    List<Future<Void>> futures = executors.invokeAll(workers);

    System.out.println("Threads started, waiting for the end");

    for (Future<Void> future : futures) {
      future.get();
    }

    executors.shutdown();
    Assert.assertTrue(executors.awaitTermination(1, TimeUnit.MINUTES));

    System.out.println("All threads have finished, shutting down server instances");

    for (ServerRun server : executeTestsOnServers) {
      if (server.isActive()) {
        printStats(server);
      }
    }

    onBeforeChecks();

    checkInsertedEntries(executeTestsOnServers);
    checkIndexedEntries(executeTestsOnServers);
  }

  protected void computeExpected(final int servers) {
    expected = writerCount * count * servers + baseCount;
  }

  protected void onBeforeChecks() throws InterruptedException {
    try {
      Thread.sleep(1000);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  protected Callable<Void> createReader(ServerRun serverRun) {
    return new Reader(serverRun);
  }

  @Override
  protected String getDatabaseURL(ServerRun server) {
    return getRemoteDatabaseURL(server);
  }

  protected String getRemoteDatabaseURL(final ServerRun server) {
    return "remote:" + server.getBinaryProtocolAddress() + "/" + getDatabaseName();
  }

  /**
   * Event called right after the database has been created and right before to be replicated to the
   * X servers
   *
   * @param db Current database
   */
  @Override
  protected void onAfterDatabaseCreation(final ODatabaseDocument db) {
    System.out.println("Creating database schema...");

    // CREATE BASIC SCHEMA
    OClass personClass = db.getMetadata().getSchema().createClass("Person");
    personClass.createProperty("id", OType.STRING);
    personClass.createProperty("name", OType.STRING);
    personClass.createProperty("birthday", OType.DATE);
    personClass.createProperty("children", OType.STRING);

    final OSchema schema = db.getMetadata().getSchema();
    OClass person = schema.getClass("Person");
    idx = person.createIndex("Person.name", INDEX_TYPE.UNIQUE, "name");

    OClass customer = schema.createClass("Customer", person);
    customer.createProperty("totalSold", OType.DECIMAL);

    OClass provider = schema.createClass("Provider", person);
    provider.createProperty("totalPurchased", OType.DECIMAL);
  }

  protected void dropIndexNode1() {
    // DISABLED WAITING FOR THE RESOLUTION OF ISSUE
    // https://github.com/orientechnologies/orientdb/issues/7335 REPRODUCIBLE WITH TEST
    // OneNodeBackupIT.java
    //    ServerRun server = serverInstance.get(0);
    //    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(server), "admin",
    // "admin").acquire();
    //    try {
    //      Object result = database.command(new OCommandSQL("drop index Person.name")).execute();
    //      System.out.println("dropIndexNode1: Node1 drop index " + result);
    //    } finally {
    //      database.close();
    //    }
    //
    //    // CHECK ON NODE 1
    //    server = serverInstance.get(1);
    //    database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
    //    try {
    //      database.getMetadata().getIndexManager().reload();
    //      Assert.assertNull(database.getMetadata().getIndexManager().getIndex("Person.name"));
    //      System.out.println("dropIndexNode1: Node2 hasn't the index too, ok");
    //    } finally {
    //      database.close();
    //    }
  }

  protected void recreateIndexNode2() {
    // DISABLED WAITING FOR THE RESOLUTION OF ISSUE
    // https://github.com/orientechnologies/orientdb/issues/7335 REPRODUCIBLE WITH TEST
    // OneNodeBackupIT.java

    //    // RE-CREATE INDEX ON NODE 1
    //    ServerRun server = serverInstance.get(1);
    //    ODatabaseDocumentTx database = poolFactory.get(getDatabaseURL(server), "admin",
    // "admin").acquire();
    //    try {
    //      Object result = database.command(new OCommandSQL("create index Person.name on Person
    // (name) unique")).execute();
    //      System.out.println("recreateIndexNode2: Node2 created index " + result);
    //      Assert.assertEquals(expected, ((Number) result).intValue());
    //    } catch (ODistributedOperationException t) {
    //
    //      for (ServerRun s : serverInstance) {
    //        final ODatabaseDocumentTx db = new
    // ODatabaseDocumentTx(getDatabaseURL(s)).open("admin", "admin");
    //
    //        try {
    //          List<ODocument> result = db.command(new OCommandSQL("select count(*) as count from
    // Person where name is not null"))
    //              .execute();
    //          Assert.assertEquals(expected, ((Number) result.get(0).field("count")).longValue());
    //
    //          final OClass person = db.getMetadata().getSchema().getClass("Person");
    //          final int[] clIds = person.getPolymorphicClusterIds();
    //
    //          long tot = 0;
    //          for (int clId : clIds) {
    //            long count = db.countClusterElements(clId);
    //            System.out.println("Cluster " + clId + " record: " + count);
    //
    //            tot += count;
    //          }
    //
    //          Assert.assertEquals(expected, tot);
    //
    //        } finally {
    //          db.close();
    //        }
    //      }
    //
    //      database.activateOnCurrentThread();
    //
    //      throw t;
    //    } finally {
    //      database.close();
    //    }
    //
    //    // CHECK ON NODE 1
    //    server = serverInstance.get(0);
    //    database = poolFactory.get(getDatabaseURL(server), "admin", "admin").acquire();
    //    try {
    //      final long indexSize =
    // database.getMetadata().getIndexManager().getIndex("Person.name").getSize();
    //      Assert.assertEquals(expected, indexSize);
    //      System.out.println("recreateIndexNode2: Node1 has the index too, ok");
    //    } finally {
    //      database.close();
    //    }

  }

  protected void checkIndexedEntries(final List<ServerRun> checkOnServers) {
    if (indexName == null) {
      return;
    }

    final Map<String, Long> result = new HashMap<String, Long>();

    for (ServerRun server : checkOnServers) {
      if (server.isActive()) {
        final ODatabaseDocumentInternal database = getDatabase(server);

        Assert.assertNotNull(
            "server " + server + " has no index " + indexName + " defined",
            database.getMetadata().getIndexManagerInternal().getIndex(database, indexName));

        try {
          final long indexSize =
              database
                  .getMetadata()
                  .getIndexManagerInternal()
                  .getIndex(database, indexName)
                  .getInternal()
                  .size();

          result.put(server.serverId, indexSize);

          if (indexSize != expected) {
            printMissingIndexEntries(server, database);
          }

        } finally {
          database.close();
        }
      }
    }

    // CHECK IF RESULT IS COHERENT BETWEEN SERVER
    String server = null;
    long value = -1;
    for (Map.Entry<String, Long> entry : result.entrySet()) {
      if (value == -1) {
        server = entry.getKey();
        value = result.values().iterator().next();
      } else if (entry.getValue() != value) {
        Assert.assertEquals(
            "Not coherent result between servers. Server "
                + entry.getKey()
                + " has "
                + entry.getValue()
                + " indexed entries, but server "
                + server
                + " has "
                + value,
            (Long) value,
            entry.getValue());
      }
    }

    // CHECK IF RESULT IS EXPECTED
    for (Map.Entry<String, Long> entry : result.entrySet()) {
      if (entry.getValue() != expected) {
        Assert.assertEquals(
            "Indexed items on server "
                + entry.getKey()
                + " are "
                + entry.getValue()
                + ", but "
                + expected
                + " was expected",
            (Long) expected,
            entry.getValue());
      }
    }
  }

  private void printMissingIndexEntries(
      final ServerRun server, final ODatabaseDocumentInternal database) {
    List<ODocument> qResult; // ERROR: CHECK WHAT'S MISSING
    int missingKeys = 0;
    for (int s = 0; s < executeTestsOnServers.size(); ++s) {
      ServerRun srv = executeTestsOnServers.get(s);
      final int srvId = Integer.parseInt(srv.serverId);

      for (int threadId = srvId * writerCount; threadId < (srvId + 1) * writerCount; ++threadId) {

        for (int i = 0; i < count; ++i) {
          final String key = "Billy" + srvId + "-" + threadId + "-" + i;

          final OIndex index =
              database.getSharedContext().getIndexManager().getIndex(database, indexName);
          try (Stream<ORID> rids = index.getInternal().getRids(key)) {
            if (!rids.findAny().isPresent()) {
              missingKeys++;
              System.out.println("Missing key: " + key + " on server: " + server);
            }
          }
        }
      }
    }
    System.out.println("Total missing keys " + missingKeys + " on server: " + server);
  }

  protected void checkInsertedEntries() {
    checkInsertedEntries(serverInstance);
  }

  protected void checkInsertedEntries(final List<ServerRun> checkOnServers) {
    final Map<OIdentifiable, StringBuilder> records =
        new LinkedHashMap<OIdentifiable, StringBuilder>((int) expected);

    int activeServers = 0;
    for (int s = 0; s < checkOnServers.size(); ++s) {
      ServerRun server = checkOnServers.get(s);
      if (!server.isActive()) continue;

      activeServers++;

      final ODatabaseDocument database = getDatabase(server);
      try {
        for (ODocument rec : database.browseClass(className)) {
          StringBuilder servers = records.get(rec);
          if (servers == null) {
            servers =
                new StringBuilder(
                    server.getServerInstance().getDistributedManager().getLocalNodeName());
            records.put(rec, servers);
          } else
            servers.append(
                "," + server.getServerInstance().getDistributedManager().getLocalNodeName());
        }

      } finally {
        database.close();
      }
    }

    for (Map.Entry<OIdentifiable, StringBuilder> entry : records.entrySet()) {
      if (entry.getValue().toString().split(",").length != activeServers) {
        System.out.println(
            "Record "
                + ((ODocument) entry.getKey().getRecord()).field("name")
                + " found only on servers "
                + entry.getValue());
      }
    }

    for (int s = 0; s < checkOnServers.size(); ++s) {
      ServerRun server = checkOnServers.get(s);
      if (!server.isActive()) continue;

      final ODatabaseDocument database = getDatabase(server);
      try {
        final long total = database.countClass(className);

        //        if (expected != total) {
        //          System.out.println("Server " +
        // server.getServerInstance().getDistributedManager().getLocalNodeName());
        //
        //          long totalClusters = 0;
        //          final OClass cls = database.getMetadata().getSchema().getClass(className);
        //          for (int clId : cls.getPolymorphicClusterIds()) {
        //            System.out.println("- cluster " + database.getClusterNameById(clId) + ": " +
        // database.countClusterElements(clId));
        //            totalClusters += database.countClusterElements(clId);
        //          }
        //          System.out.println("Total from clusters: " + totalClusters);
        //
        //          final List<String> orderedNames = new ArrayList<String>();
        //          for (Map.Entry<OIdentifiable, StringBuilder> entry : records.entrySet()) {
        //            orderedNames.add(
        //                (String) ((ODocument) entry.getKey().getRecord()).field("name") + ": " +
        // ((ODocument) entry.getKey().getRecord())
        //                    .getIdentity());
        //          }
        //
        //          Collections.sort(orderedNames);
        //          for (String entry : orderedNames) {
        //            System.out.println("- record " + entry);
        //          }
        //        }

        Assert.assertEquals(
            "Server " + server.getServerId() + " count is not what was expected", expected, total);

      } finally {
        database.close();
      }
    }
  }

  protected Callable<Void> createWriter(
      final int serverId, final int threadId, final ServerRun serverRun) {
    return new BaseWriter(serverId, threadId, serverRun);
  }

  private void printStats(final ServerRun serverRun) {
    final ODatabaseDocument database = getDatabase(serverRun);

    try {
      List<ODocument> result =
          database.query(new OSQLSynchQuery<OIdentifiable>("select count(*) from " + className));

      final String name = database.getURL();

      System.out.println(
          "\nReader "
              + name
              + " sql count: "
              + result.get(0)
              + " counting class: "
              + database.countClass(className));

    } catch (Exception e) {

    } finally {
      database.close();
    }
  }
}
