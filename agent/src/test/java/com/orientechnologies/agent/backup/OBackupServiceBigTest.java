/*
 * Copyright 2015 OrientDB LTD (info(at)orientdb.com)
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 *
 *   For more information: http://www.orientdb.com
 */

package com.orientechnologies.agent.backup;

import static junit.framework.Assert.assertNotNull;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.agent.services.backup.OBackupTask;
import com.orientechnologies.agent.services.backup.log.OBackupLogType;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import java.io.File;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/*
 * The full backup displacment problem happens, when an incremental backup takes so long to overlap
 * with a schedule of a full backup and the next backup schedule is an incremental one again. In
 * that case, the full backup could be displaced for several hours or days, until an incremental
 * backup is short enough to allow the execution of a full backup. This kind of problems is
 * difficult to reproduce, hence the following measures have been taken: (1) more frequent
 * INCREMENTAL_BACKUP, than FULL_BACKUP, (2) base fill of database with documents for slowing down
 * the backups and (3) async inserting of data by simulating several concurrent users.
 *
 * <p>When the problem occurs, the log shows one FULL_BACKUP followed by several INCREMENTAL_BACKUP
 * until the latches are counted down. The solution shows a FULL_BACKUP, followed by
 * INCREMENTAL_BACKUP and FULL_BACKUP, letting the latter close to its schedule (cf. changes,
 * comments in @see
 * com.orientechnologies.agent.services.backup.strategy.OBackupStrategyMixBackup#scheduleNextExecution()).
 */
public class OBackupServiceBigTest {
  private OServer server;

  private final String DB_NAME = "backupBigDBTest";
  private final String DB_NAME_RESTORED = "backupBigDBTestRestored";
  private final String BACKUP_PATH =
      System.getProperty("buildDirectory", "target")
          + File.separator
          + "databases"
          + File.separator
          + DB_NAME;

  private final String BACKUP_CONF_DIR =
      System.getProperty("buildDirectory", "target") + File.separator + "config" + File.separator;
  private final String BACKUP_CONF = BACKUP_CONF_DIR + "backups.json";

  private OBackupService manager;
  private OrientDB orient;
  private ODatabasePool pool;

  // for concurrent inserts
  private static int NUMBER_THREADS = 4;
  private static final ExecutorService executor = Executors.newFixedThreadPool(NUMBER_THREADS);
  final Map<String, Future<?>> futures = new HashMap<>();
  private static int TIMEOUT_IN_SEC = 86400;

  @Before
  public void bootOrientDB() throws Exception {
    OFileUtils.deleteRecursively(new File(BACKUP_PATH));
    OFileUtils.delete(new File(BACKUP_CONF));
    new File(BACKUP_CONF_DIR).mkdirs();

    final ODocument cfg = new ODocument();
    cfg.field("backups", new ArrayList<ODocument>());
    OIOUtils.writeFile(new File(BACKUP_CONF), cfg.toJSON("prettyPrint"));

    final InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);

    orient = server.getContext();
    orient.execute(
        "create database " + DB_NAME + " plocal users(admin identified by 'admin' role admin)");
    server.activate();
    server
        .getSystemDatabase()
        .executeInDBScope(
            iArgument -> {
              iArgument.command("delete from OBackupLog");
              return null;
            });

    final OEnterpriseAgent agent = server.getPluginByClass(OEnterpriseAgent.class);
    manager = agent.getServiceByClass(OBackupService.class).get();
    final ODocument configuration = manager.getConfiguration();
    configuration.field("backups", new ArrayList<>());

    orient.open(DB_NAME, "admin", "admin");
    if (pool == null) {
      pool = new ODatabasePool(orient, DB_NAME, "admin", "admin");
    }
    // fill database for reproducing the full backup displacment problem.
    final int numberDocuments = 300000;
    insertDocuments(numberDocuments, 10);
    verifyInsert(numberDocuments);
  }

  private void insertDocuments(final int numberDocuments, final int numberValues) {
    System.out.println(
        "Start inserting documents for backup (docs="
            + numberDocuments
            + ", values="
            + numberValues
            + ").");
    try (final ODatabaseSession session = pool.acquire()) {
      session.createClassIfNotExist("backups");

      final Map<String, Integer> values = new HashMap<>();
      for (int i = 0; i < numberValues; i++) {
        values.put("field" + i, i);
      }

      for (int i = 0; i < numberDocuments; i++) {
        // create outside of tx
        final OElement element = session.newInstance("myTable");

        session.begin();
        values.entrySet().stream().forEach(e -> element.setProperty(e.getKey(), e.getValue()));
        element.save();
        // slower with continuous changes for reproducing the full backup displacment problem.
        session.commit();
      }
    } catch (final Exception e) {
      System.err.println("Unable to insert data: " + e.getMessage());
    }
    System.out.println("Documents inserted for backup.");
  }

  private void verifyInsert(final int expectedNumberDocuments) {
    try (final ODatabaseSession session = pool.acquire();
        final OResultSet rs = session.query("select * from myTable")) {
      Assert.assertEquals(
          "Document insert verification failed", expectedNumberDocuments, rs.stream().count());
    } catch (final Exception e) {
      System.err.println("Unable to read data: " + e.getMessage());
    }
    System.out.println("Verified documents properly inserted.");
  }

  @After
  public void tearDownOrientDB() {
    final OrientDB orientDB = server.getContext();
    if (orientDB.exists(DB_NAME)) {
      orientDB.drop(DB_NAME);
    }
    if (orientDB.exists(DB_NAME_RESTORED)) {
      orientDB.drop(DB_NAME_RESTORED);
    }
    if (pool != null) {
      pool.close();
    }
    if (server != null) {
      server.shutdown();
    }
    Orient.instance().shutdown();
    Orient.instance().startup();
    OFileUtils.deleteRecursively(new File(BACKUP_PATH));
  }

  private int calculateToDelete(
      final List<ODocument> list, @SuppressWarnings("SameParameterValue") int start) {
    int counter = 0;
    Long last = null;
    do {
      ODocument document = list.get(start);
      Long val = document.field("unitId");
      if (last == null) {
        last = val;
      }
      if (!last.equals(val)) {
        break;
      }
      start--;
      counter++;
    } while (start >= 0);
    return counter;
  }

  @Test
  public void ensureFullBackupAfterCancelledFullBackup() throws InterruptedException {
    // Tight incremental schedule with occasional full backups.
    // Every 1 seconds starting at :00 second after the minute
    final ODocument modes = getBackupMode("INCREMENTAL_BACKUP", "0/1 * * * * ?");
    // Every 4 seconds starting at :00 second after the minute
    addBackupMode(modes, "FULL_BACKUP", "0/4 * * * * ?");
    final ODocument backup = configureBackup(modes);

    final ODocument cfg = manager.addBackupAndSchedule(backup);
    final String uuid = cfg.field("uuid");
    try {
      final OBackupTask task = manager.getTask(uuid);
      // have continuous writing of data for reproducing the full backup displacment problem.
      startAsyncDocumentInsertions();

      final CountDownLatch latch = new CountDownLatch(17);
      task.registerListener(
          (cfg1, log) -> {
            latch.countDown();
            return latch.getCount() > 0;
          });
      latch.await();
      // task.stop();
      checkStoppingAsyncDatabaseInserter();

      final ODocument logs = manager.logs(uuid, 1, 50, new HashMap<>());
      assertNotNull(logs);
      assertNotNull(logs.field("logs"));
      final List<ODocument> list = logs.field("logs");

      int backupOperationsStarted = 0;
      int backupOperationsFinished = 0;
      int fullBackupFinished = 0;

      for (final ODocument log : list) {
        final String operation = log.field("op");
        final String mode = log.field("mode");
        if (operation.equals("BACKUP_STARTED")) {
          backupOperationsStarted++;
        }
        if (operation.equals("BACKUP_FINISHED")) {
          backupOperationsFinished++;
          if (mode.equals("FULL_BACKUP")) {
            fullBackupFinished++;
          }
        }
      }
      Assert.assertEquals("", backupOperationsStarted, backupOperationsFinished);
      System.out.println("Full backups finished:" + fullBackupFinished);
      // first backup is always full. Subsequent ones are incremental. But at least one more should
      // be full until the end of the latches. Note that this is an approximation due to the task
      // scheduling.
      Assert.assertTrue("Full backups not sufficient", fullBackupFinished >= 2);

      checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());
      deleteAndCheck(uuid, list, 17, 18 - calculateToDelete(list, 17));
    } finally {
      manager.removeBackup(uuid);
    }
  }

  private void startAsyncDocumentInsertions() {
    for (int i = 0; i < NUMBER_THREADS; i++) {
      futures.put(
          "writer" + i,
          executor.submit(
              new Runnable() {
                @Override
                public void run() {
                  insertDocuments(500000, 10);
                }
              }));
    }
  }

  private void checkStoppingAsyncDatabaseInserter() {
    int count = 0;
    for (final Map.Entry<String, Future<?>> future : futures.entrySet()) {
      try {
        future.getValue().get(TIMEOUT_IN_SEC, TimeUnit.SECONDS);
        count++;
        System.out.println("progress: " + count + " of " + futures.size());
      } catch (final InterruptedException | ExecutionException | TimeoutException e) {
        System.out.println(
            "Failed task "
                + count
                + " of "
                + futures.size()
                + " in "
                + future.getKey()
                + e.getMessage());
        future.getValue().cancel(true);
      }
    }
    System.out.println("Futures completed " + count + " of " + futures.size());
    executor.shutdown();
  }

  private ODocument getBackupMode(final String backupMode, final String schedule) {
    final ODocument modes = new ODocument();
    addBackupMode(modes, backupMode, schedule);
    return modes;
  }

  private void addBackupMode(
      final ODocument modes, final String backupMode, final String schedule) {
    final ODocument incrementalMode = new ODocument();
    modes.field(backupMode, incrementalMode);
    incrementalMode.field("when", schedule);
  }

  private ODocument configureBackup(final ODocument fullBackModes) {
    final ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", fullBackModes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);
    return backup;
  }

  private void deleteAndCheck(String uuid, List<ODocument> list, int index, long expected) {
    ODocument doc = list.get(index);
    long unitId = doc.field("unitId");
    long txId = doc.field("txId");
    manager.deleteBackup(uuid, unitId, txId);
    checkExpected(expected, uuid);
  }

  private void checkExpected(long expected, String uuid) {
    String query;
    if (uuid != null) {
      query = "select count(*) as count from OBackupLog where uuid = ?";
    } else {
      query = "select count(*) as count from OBackupLog";
    }
    @SuppressWarnings("unchecked")
    List<OResult> execute =
        (List<OResult>)
            server
                .getSystemDatabase()
                .execute(iArgument -> iArgument.stream().collect(Collectors.toList()), query, uuid);
    assertThat(execute.get(0).<Long>getProperty("count")).isEqualTo(expected);
  }

  private void checkNoOp(Collection<ODocument> list, String op) {
    for (ODocument log : list) {
      assertNotEquals(op, log.field("op"));
    }
  }
}
