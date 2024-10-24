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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.agent.services.backup.OBackupTask;
import com.orientechnologies.agent.services.backup.log.OBackupLogType;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import java.io.File;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/** Created by Enrico Risa on 22/03/16. */
public class OBackupServiceTestIT {
  private OServer server;

  private final String DB_NAME = "backupDBTest";
  private final String DB_NAME_RESTORED = "backupDBTestRestored";
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
  private OrientDB orientDB;

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

    orientDB = server.getContext();
    orientDB.execute(
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
  }

  @After
  public void tearDownOrientDB() {
    OrientDB orientDB = server.getContext();
    if (orientDB.exists(DB_NAME)) orientDB.drop(DB_NAME);

    if (orientDB.exists(DB_NAME_RESTORED)) orientDB.drop(DB_NAME_RESTORED);

    if (server != null) server.shutdown();

    Orient.instance().shutdown();
    Orient.instance().startup();

    OFileUtils.deleteRecursively(new File(BACKUP_PATH));
  }

  private int calculateToDelete(List<ODocument> list, int start) {

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
  public void backupFullTest() throws InterruptedException {
    final ODocument fullBackModes = getBackupMode("FULL_BACKUP", "0/5 * * * * ?");
    final ODocument backup = configureBackup(fullBackModes);

    final ODocument cfg = manager.addBackupAndSchedule(backup);
    final String uuid = cfg.field("uuid");
    try {
      final OBackupTask task = manager.getTask(uuid);
      final CountDownLatch latch = new CountDownLatch(5);
      task.registerListener(
          (cfg1, log) -> {
            latch.countDown();
            return latch.getCount() > 0;
          });
      latch.await();
      // task.stop();

      final ODocument logs = manager.logs(uuid, 1, 50, new HashMap<>());
      assertNotNull(logs);
      assertNotNull(logs.field("logs"));

      List<ODocument> list = logs.field("logs");
      assertEquals(6, list.size());

      checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());
      deleteAndCheck(uuid, list, 5, 3);

      task.getStrategy().retainLogs(-1);
      list = getLogs(uuid);
      assertEquals(0, list.size());

      list = logs.field("logs");
      checkEmptyPaths(list);
    } finally {
      manager.removeBackup(uuid);
    }
  }

  @Test
  public void backupFullIncrementalMixTest() throws InterruptedException {
    final ODocument modes = getBackupMode("FULL_BACKUP", "0/5 * * * * ?");
    addBackupMode(modes, "INCREMENTAL_BACKUP", "0/2 * * * * ?");
    final ODocument backup = configureBackup(modes);

    final ODocument cfg = manager.addBackupAndSchedule(backup);
    final String uuid = cfg.field("uuid");
    try {
      final OBackupTask task = manager.getTask(uuid);
      final CountDownLatch latch = new CountDownLatch(17);
      task.registerListener(
          (cfg1, log) -> {
            latch.countDown();
            return latch.getCount() > 0;
          });
      latch.await();
      // task.stop();

      final ODocument logs = manager.logs(uuid, 1, 50, new HashMap<>());
      assertNotNull(logs);
      assertNotNull(logs.field("logs"));

      final List<ODocument> list = logs.field("logs");
      assertEquals(18, list.size());

      checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());
      deleteAndCheck(uuid, list, 17, 18 - calculateToDelete(list, 17));
    } finally {
      manager.removeBackup(uuid);
    }
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

  private void checkEmptyPaths(List<ODocument> list) {

    for (ODocument log : list) {
      if (log.field("op").equals(OBackupLogType.BACKUP_FINISHED.toString())) {

        String path = log.field("path");

        File f = new File(path);

        assertFalse(f.exists());
      }
    }
  }

  private List<ODocument> getLogs(String uuid) {
    ODocument logs = manager.logs(uuid, 1, 50, new HashMap<>());
    assertNotNull(logs);
    assertNotNull(logs.field("logs"));

    return logs.field("logs");
  }

  @Test
  public void backupIncrementalTest() throws InterruptedException {

    checkExpected(0, null);
    ODocument modes = new ODocument();

    addBackupMode(modes, "INCREMENTAL_BACKUP", "0/5 * * * * ?");

    ODocument backup = configureBackup(modes);

    ODocument cfg = manager.addBackupAndSchedule(backup);

    String uuid = cfg.field("uuid");

    try {
      OBackupTask task = manager.getTask(uuid);

      final CountDownLatch latch = new CountDownLatch(5);
      task.registerListener(
          (cfg1, log) -> {
            latch.countDown();
            return latch.getCount() > 0;
          });
      latch.await();

      ODocument logs = manager.logs(uuid, 1, 50, new HashMap<>());
      assertNotNull(logs);
      assertNotNull(logs.field("logs"));

      List<ODocument> list = logs.field("logs");
      assertEquals(6, list.size());

      checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());

      checkSameUnitUids(list);

      deleteAndCheck(uuid, list, 5, 0);

      checkEmptyPaths(list);
    } finally {
      manager.removeBackup(uuid);
    }
  }

  @Test
  public void backupRestoreIncrementalTest() throws InterruptedException {

    checkExpected(0, null);
    ODocument modes = new ODocument();

    addBackupMode(modes, "INCREMENTAL_BACKUP", "0/5 * * * * ?");

    final ODocument backup = configureBackup(modes);

    ODocument cfg = manager.addBackupAndSchedule(backup);

    String uuid = cfg.field("uuid");

    try {
      OBackupTask task = manager.getTask(uuid);

      final CountDownLatch latch = new CountDownLatch(5);
      task.registerListener(
          (cfg1, log) -> {
            latch.countDown();
            return latch.getCount() > 0;
          });
      latch.await();
      task.stop();

      ODocument logs = manager.logs(uuid, 1, 50, new HashMap<>());
      assertNotNull(logs);
      assertNotNull(logs.field("logs"));

      List<ODocument> list = logs.field("logs");
      assertEquals(6, list.size());

      checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());

      checkSameUnitUids(list);

      CountDownLatch restoreLatch = new CountDownLatch(2);

      task.registerListener(
          (cfg1, log) -> {
            restoreLatch.countDown();
            return true;
          });

      long unitId = list.get(0).field("unitId");

      ODocument restoreCfg =
          new ODocument().field("target", DB_NAME_RESTORED).field("unitId", unitId);

      task.restore(restoreCfg);

      restoreLatch.await();

      deleteAndCheck(uuid, list, 5, 0);

      checkEmptyPaths(list);

      try (ODatabaseSession open = orientDB.open(DB_NAME_RESTORED, "admin", "admin")) {
        long oUser = open.countClass("OUser");
        Assert.assertEquals(1, oUser);
      }

    } finally {
      manager.removeBackup(uuid);
    }
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
    List<OResult> execute =
        (List<OResult>)
            server
                .getSystemDatabase()
                .execute(iArgument -> iArgument.stream().collect(Collectors.toList()), query, uuid);
    assertEquals((long) execute.get(0).<Long>getProperty("count"), expected);
  }

  private void checkSameUnitUids(Collection<ODocument> list) {

    if (list.size() > 0) {
      Long unitId = null;
      for (ODocument d : list) {
        if (unitId == null) {
          unitId = d.field("unitId");
        } else {
          assertEquals(unitId, d.field("unitId"));
        }
      }
    } else {
      fail();
    }
  }

  private void checkNoOp(Collection<ODocument> list, String op) {
    for (ODocument log : list) {
      assertNotEquals(op, log.field("op"));
    }
  }
}
