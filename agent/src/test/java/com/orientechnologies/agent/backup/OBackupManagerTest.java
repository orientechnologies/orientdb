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
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashMap;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.fail;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

/**
 * Created by Enrico Risa on 22/03/16.
 */

public class OBackupManagerTest {

  private OServer             server;

  private final String        DB_NAME     = "backupDB";
  private final String        BACKUP_PATH = System.getProperty("java.io.tmpdir") + File.separator + DB_NAME;
  private ODatabaseDocumentTx db;

  private OBackupManager      manager;

  @Before
  public void bootOrientDB() {

    try {
      InputStream stream = ClassLoader.getSystemResourceAsStream("orientdb-server-config.xml");
      server = OServerMain.create();
      server.startup(stream);
      server.activate();
      server.getSystemDatabase().execute(new OCallable<Object, Object>() {
        @Override
        public Object call(Object iArgument) {
          return null;
        }
      }, "delete from OBackupLog");

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

  // {
  // "dbName": "test",
  // "modes": {"INCREMENTAL_BACKUP":{"when":"0 0/5 * * * ?"},"FULL_BACKUP":{"when":"0 0/10 * * * ?"}},
  // "directory": "/tmp/tests/Backup",
  // "uuid": "d8bb1c0e-133f-4c0c-9018-930d71cdb485",
  // "enabled": false,
  // "retentionDays": 30
  // }

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

    OBackupTask task = manager.getTask(uuid);
    Thread.sleep(10000);

    task.stop();


    ODocument logs = manager.logs(uuid, 1, 50, new HashMap<String, String>());
    assertNotNull(logs);
    assertNotNull(logs.field("logs"));

    Collection<ODocument> list = logs.field("logs");
    assertEquals(7, list.size());

    checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());

  }

  @Test
  public void backupIncrementalTest() throws InterruptedException {

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

    OBackupTask task = manager.getTask(uuid);

    Thread.sleep(10000);

    task.stop();

    ODocument logs = manager.logs(uuid, 1, 50, new HashMap<String, String>());
    assertNotNull(logs);
    assertNotNull(logs.field("logs"));

    Collection<ODocument> list = logs.field("logs");
    assertEquals(7, list.size());

    checkNoOp(list, OBackupLogType.BACKUP_ERROR.toString());

    checkSameUnitUids(list);

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
