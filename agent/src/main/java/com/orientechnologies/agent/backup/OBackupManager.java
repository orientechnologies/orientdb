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

import com.orientechnologies.agent.backup.log.OBackupDBLogger;
import com.orientechnologies.agent.backup.log.OBackupDiskLogger;
import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogger;
import com.orientechnologies.agent.backup.strategy.OBackupStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerLifecycleListener;
import com.orientechnologies.orient.server.OServerMain;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Created by Enrico Risa on 22/03/16.
 */
public class OBackupManager implements OServerLifecycleListener {

  private final OServer              server;
  OBackupConfig                      config;
  OBackupLogger                      logger;
  protected Map<String, OBackupTask> tasks = new ConcurrentHashMap<String, OBackupTask>();

  public OBackupManager() {

    this(OServerMain.server());
  }

  public OBackupManager(OServer server) {
    this(server, new OBackupConfig().load());
  }

  private void initTasks() {

    Collection<ODocument> backups = config.backups();
    for (ODocument backup : backups) {
      OBackupStrategy strategy = config.strategy(backup, logger);
      tasks.put((String) backup.field(OBackupConfig.ID), new OBackupTask(strategy));
    }
  }

  private void initLogger() {
    if (server.getSystemDatabase().exists()) {
      logger = new OBackupDBLogger();
    } else {
      logger = new OBackupDiskLogger();
    }
  }

  public OBackupManager(OServer server, OBackupConfig config) {
    this.config = config;

    this.server = server;
    server.registerLifecycleListener(this);
  }

  public ODocument getConfiguration() {
    return config.getConfig();
  }

  public ODocument addBackup(ODocument doc) {
    ODocument backup = config.addBackup(doc);
    OBackupStrategy strategy = config.strategy(backup, logger);
    tasks.put((String) doc.field(OBackupConfig.ID), new OBackupTask(strategy));
    return backup;
  }

  public void restoreBackup(String uuid, ODocument doc) {

    OBackupTask oBackupTask = tasks.get(uuid);

    oBackupTask.restore(doc);

  }

  public void changeBackup(String uuid, ODocument doc) {

    config.changeBackup(uuid, doc);
    OBackupTask oBackupTask = tasks.get(uuid);
    oBackupTask.changeConfig(config, doc);
  }

  public void removeBackup(String uuid, ODocument doc) {
    config.removeBackup(uuid, doc);
  }

  public ODocument logs(String uuid, int page, int pageSize) {
    ODocument history = new ODocument();
    try {
      List<OBackupLog> byUUID = logger.findAllLatestByUUID(uuid, page, pageSize);
      List<ODocument> docs = new ArrayList<ODocument>();
      for (OBackupLog oBackupLog : byUUID) {
        docs.add(oBackupLog.toDoc());
      }
      history.field("logs", docs);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return history;
  }

  public ODocument logs(String uuid, Long unitId, int page, int pageSize) {
    ODocument history = new ODocument();
    try {
      List<OBackupLog> byUUID = logger.findByUUIDAndUnitId(uuid, unitId, page, pageSize);
      List<ODocument> docs = new ArrayList<ODocument>();
      for (OBackupLog oBackupLog : byUUID) {
        docs.add(oBackupLog.toDoc());
      }
      history.field("logs", docs);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return history;
  }

  @Override
  public void onBeforeActivate() {

  }

  @Override
  public void onAfterActivate() {
    initLogger();
    initTasks();
  }

  @Override
  public void onBeforeDeactivate() {

  }

  @Override
  public void onAfterDeactivate() {

  }
}
