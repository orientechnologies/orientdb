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
import static junit.framework.Assert.fail;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.*;

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.services.backup.OBackupService;
import com.orientechnologies.agent.services.backup.OBackupTask;
import com.orientechnologies.agent.services.backup.log.OBackupLogType;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseType;
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
public class OBackupServiceTest {

  private OServer server;

  private final String DB_NAME = "backupDBTest";
  private final String DB_NAME_RESTORED = "backupDBTestRestored";
  private final String BACKUP_PATH =
      System.getProperty("buildDirectory", "target")
          + File.separator
          + "databases"
          + File.separator
          + DB_NAME;

  private final String BACKUP_CONF =
      System.getProperty("buildDirectory", "target")
          + File.separator
          + "config"
          + File.separator
          + "backups.json";

  private OBackupService manager;
  private OrientDB orientDB;

  @Before
  public void bootOrientDB() throws Exception {
    OFileUtils.deleteRecursively(new File(BACKUP_PATH));
    OFileUtils.delete(new File(BACKUP_CONF));

    ODocument cfg = new ODocument();
    cfg.field("backups", new ArrayList<ODocument>());
    OIOUtils.writeFile(new File(BACKUP_CONF), cfg.toJSON("prettyPrint"));

    InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
    server = OServerMain.create(false);
    server.startup(stream);

    orientDB = server.getContext();
    orientDB.create(DB_NAME, ODatabaseType.PLOCAL);

    server.activate();

    server
        .getSystemDatabase()
        .executeInDBScope(
            iArgument -> {
              iArgument.command("delete from OBackupLog");
              return null;
            });

    OEnterpriseAgent agent = server.getPluginByClass(OEnterpriseAgent.class);

    manager = agent.getServiceByClass(OBackupService.class).get();

    ODocument configuration = manager.getConfiguration();

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

  @Test
  public void backupFullIncrementalMixTest() throws InterruptedException {

    ODocument modes = new ODocument();

    ODocument mode = new ODocument();
    modes.field("FULL_BACKUP", mode);
    mode.field("when", "0/5 * * * * ?");

    ODocument incrementalMode = new ODocument();
    modes.field("INCREMENTAL_BACKUP", incrementalMode);
    incrementalMode.field("when", "0/2 * * * * ?");

    ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", modes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);

    ODocument cfg = manager.addBackup(backup);

    String uuid = cfg.field("uuid");

    try {
      final OBackupTask task = manager.getTask(uuid);

      final CountDownLatch latch = new CountDownLatch(17);
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
      assertEquals(18, list.size());

      checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());

      deleteAndCheck(uuid, list, 17, 18 - calculateToDelete(list, 17));
    } finally {
      manager.removeBackup(uuid);
    }
  }

  private int calculateToDelete(
      List<ODocument> list, @SuppressWarnings("SameParameterValue") int start) {

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

    ODocument modes = new ODocument();

    ODocument mode = new ODocument();
    modes.field("FULL_BACKUP", mode);
    mode.field("when", "0/5 * * * * ?");

    ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", modes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);

    ODocument cfg = manager.addBackup(backup);

    String uuid = cfg.field("uuid");

    try {
      final OBackupTask task = manager.getTask(uuid);

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

    ODocument mode = new ODocument();
    modes.field("INCREMENTAL_BACKUP", mode);
    mode.field("when", "0/5 * * * * ?");

    ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", modes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);

    ODocument cfg = manager.addBackup(backup);

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

    ODocument mode = new ODocument();
    modes.field("INCREMENTAL_BACKUP", mode);
    mode.field("when", "0/5 * * * * ?");

    ODocument backup = new ODocument();
    backup.field("dbName", DB_NAME);
    backup.field("directory", BACKUP_PATH);
    backup.field("modes", modes);
    backup.field("enabled", true);
    backup.field("retentionDays", 30);

    ODocument cfg = manager.addBackup(backup);

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
        Assert.assertEquals(3, oUser);
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
    @SuppressWarnings("unchecked")
    List<OResult> execute =
        (List<OResult>)
            server
                .getSystemDatabase()
                .execute(iArgument -> iArgument.stream().collect(Collectors.toList()), query, uuid);
    assertThat(execute.get(0).<Long>getProperty("count")).isEqualTo(expected);
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
