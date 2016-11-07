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

import com.orientechnologies.agent.OEnterpriseAgent;
import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import static org.junit.Assert.*;

/**
 * Created by Enrico Risa on 22/03/16.
 */

public class OBackupManagerTest {

  private final String DB_NAME     = "backupDB";
  private final String BACKUP_PATH = System.getProperty("java.io.tmpdir") + File.separator + DB_NAME;
  private OServer             server;
  private ODatabaseDocumentTx db;

  private OBackupManager manager;

  @Before
  public void bootOrientDB() {

    try {
      InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
      server = OServerMain.create(false);
      server.startup(stream);
      server.activate();

      server.getSystemDatabase().executeInDBScope(new OCallable<Void, ODatabase>() {
        @Override
        public Void call(ODatabase iArgument) {

          iArgument.command(new OCommandSQL("delete from OBackupLog")).execute();
          return null;
        }
      });

      db = new ODatabaseDocumentTx("plocal:" + server.getDatabaseDirectory() + File.separator + DB_NAME);

      if (db.exists()) {

        db.drop();
      } else {
        db.create();
      }

      OEnterpriseAgent agent = server.getPluginByClass(OEnterpriseAgent.class);

      manager = agent.getBackupManager();

    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @After
  public void tearDownOrientDB() {
    db.drop();
    server.shutdown();
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
      task.registerListener(new OBackupListener() {
        @Override
        public Boolean onEvent(ODocument cfg, OBackupLog log) {
          latch.countDown();
          return latch.getCount() > 0;

        }
      });
      latch.await();
      ODocument logs = manager.logs(uuid, 1, 50, new HashMap<String, String>());
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
      task.registerListener(new OBackupListener() {
        @Override
        public Boolean onEvent(ODocument cfg, OBackupLog log) {
          latch.countDown();
          return latch.getCount() > 0;

        }
      });
      latch.await();
      ODocument logs = manager.logs(uuid, 1, 50, new HashMap<String, String>());
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

  protected List<ODocument> getLogs(String uuid) {
    ODocument logs = manager.logs(uuid, 1, 50, new HashMap<String, String>());
    assertNotNull(logs);
    assertNotNull(logs.field("logs"));

    List<ODocument> list = logs.field("logs");
    return list;
  }

  @Test
  public void backupIncrementalTest() throws InterruptedException {

    checkExpected(0);
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
      task.registerListener(new OBackupListener() {
        @Override
        public Boolean onEvent(ODocument cfg, OBackupLog log) {
          latch.countDown();
          return latch.getCount() > 0;

        }
      });
      latch.await();

      ODocument logs = manager.logs(uuid, 1, 50, new HashMap<String, String>());
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

  private void deleteAndCheck(String uuid, List<ODocument> list, int index, long expected) {
    ODocument doc = list.get(index);
    long unitId = doc.field("unitId");
    long txId = doc.field("txId");
    manager.deleteBackup(uuid, unitId, txId);
    checkExpected(expected);
  }

  private void checkExpected(long expected) {
    List<ODocument> execute = (List<ODocument>) server.getSystemDatabase().execute(new OCallable<Object, Object>() {
      @Override
      public Object call(Object iArgument) {
        return iArgument;
      }
    }, "select count(*) from OBackupLog");

    long count = execute.get(0).field("count");
    assertEquals(expected, count);
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
