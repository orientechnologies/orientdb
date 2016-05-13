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

package com.orientechnologies.agent.backup.strategy;

import com.orientechnologies.agent.backup.OBackupConfig;
import com.orientechnologies.agent.backup.OBackupListener;
import com.orientechnologies.agent.backup.log.*;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.metadata.security.OSecurityNull;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;

import java.io.File;
import java.io.IOException;
import java.util.Date;

/**
 * Created by Enrico Risa on 25/03/16.
 */
public abstract class OBackupStrategy {

  protected ODocument     cfg;
  protected OBackupLogger logger;

  public OBackupStrategy(ODocument cfg, OBackupLogger logger) {
    this.cfg = cfg;
    this.logger = logger;
  }

  public OBackupStartedLog startBackup() throws IOException {

    OBackupLog last = logger.findLast(OBackupLogType.BACKUP_SCHEDULED, getUUID());

    long txId;
    long unitId;
    if (last != null) {
      unitId = last.getUnitId();
      txId = last.getTxId();
    } else {
      unitId = logger.nextOpId();
      txId = logger.nextOpId();
    }
    return new OBackupStartedLog(unitId, txId, getUUID(), getDbName(), getMode().toString());
  }

  public OBackupFinishedLog endBackup(long unitId, long opsId) {
    return new OBackupFinishedLog(unitId, opsId, getUUID(), getDbName(), getMode().toString());
  }

  // Backup
  public void doBackup(OBackupListener listener) throws IOException {

    OBackupStartedLog start = startBackup();
    logger.log(start);
    listener.onEvent(cfg, start);

    OBackupFinishedLog end;
    try {
      end = doBackup(start);
      logger.log(end);
    } catch (Exception e) {
      OBackupErrorLog error = new OBackupErrorLog(start.getUnitId(), start.getTxId(), getUUID(), getDbName(), getMode().toString());
      error.setMessage(e.getMessage());
      logger.log(error);
      listener.onEvent(cfg, error);
      return;
    }

    listener.onEvent(cfg, end);
  }

  public void doRestore(OBackupListener listener, ODocument doc) {
    String databaseName = doc.field("target");
    Long unitId = doc.field("unitId");
    OServer server = OServerMain.server();

    ODatabaseDocumentTx database = null;
    try {
      OBackupFinishedLog finished = (OBackupFinishedLog) logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID(), unitId);
      String url = "plocal:" + server.getDatabaseDirectory() + databaseName;
      database = new ODatabaseDocumentTx(url);
      if (!database.exists()) {
        database.create(finished.getPath());
        // database.incrementalRestore(finished.getPath());
      }
    } catch (Exception e) {

    } finally {
      if (database != null) {
        database.close();
      }
    }
  }

  protected OBackupFinishedLog doBackup(OBackupStartedLog start) {

    ODatabaseDocumentTx db = null;
    try {
      db = getDatabase();
      db.activateOnCurrentThread();
      String path = calculatePath();
      String fName = db.incrementalBackup(path);
      OBackupFinishedLog end = endBackup(start.getUnitId(), start.getTxId());
      end.setFileName(fName);
      end.setPath(path);
      end.fileSize = calculateFileSize(path + File.separator + fName);
      end.elapsedTime = end.getTimestamp() - start.getTimestamp();
      return end;
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

  protected long calculateFileSize(String path) {
    File f = new File(path);
    return f.length();
  }

  public abstract OAutomaticBackup.MODE getMode();

  protected abstract String calculatePath();

  public abstract Date scheduleNextExecution();

  public OBackupLogger getLogger() {
    return logger;
  }

  protected ODatabaseDocumentTx getDatabase() {

    String dbName = cfg.field(OBackupConfig.DBNAME);

    OServer server = OServerMain.server();

    String url = server.getAvailableStorageNames().get(dbName);

    ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);

    db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), OSecurityNull.class);
    db.open("admin", "aaa");

    return db;
  }

  public String getUUID() {
    return cfg.field(OBackupConfig.ID);
  }

  public String getDbName() {
    return cfg.field(OBackupConfig.DBNAME);
  }

  protected OBackupScheduledLog lastUnfiredSchedule() {
    OBackupLog lastSchedule = null;
    try {
      lastSchedule = logger.findLast(OBackupLogType.BACKUP_SCHEDULED, getUUID());
      if (lastSchedule != null) {
        OBackupLog lastBackup = logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID());
        if (lastBackup != null && lastBackup.getTxId() == lastSchedule.getTxId()) {
          lastSchedule = null;
        }
      }
    } catch (Exception e) {
    }
    return (OBackupScheduledLog) lastSchedule;
  }
}
