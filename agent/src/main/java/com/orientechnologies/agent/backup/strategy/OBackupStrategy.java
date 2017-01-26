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
import com.orientechnologies.agent.backup.OBackupTask;
import com.orientechnologies.agent.backup.log.*;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.OServerMain;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.Calendar;
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
      StringWriter sw = new StringWriter();
      PrintWriter pw = new PrintWriter(sw);
      e.printStackTrace(pw);
      error.setMessage(e.getMessage());
      error.setStackTrace(sw.toString());
      logger.log(error);
      listener.onEvent(cfg, error);
      return;
    }

    listener.onEvent(cfg, end);
  }

  public void doRestore(final OBackupListener listener, ODocument doc) {

    final String databaseName = doc.field("target");
    Long unitId = doc.field("unitId");
    OServer server = OServerMain.server();
    ODatabaseDocument database = null;

    final String url = "plocal:" + server.getDatabaseDirectory() + databaseName;
    database = new ODatabaseDocumentTx(url);

    if (database.exists()) {
      throw new IllegalArgumentException("Cannot restore the backup to an existing database (" + databaseName + ").");
    }
    try {

      final OBackupFinishedLog finished = (OBackupFinishedLog) logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID(), unitId);

      File f = new File(finished.getPath());
      if (!f.exists()) {
        throw new IllegalArgumentException("Cannot restore the backup from path (" + finished.getPath() + ").");
      }

      new Thread(new Runnable() {
        @Override
        public void run() {
          doRestoreBackup(url, finished, databaseName, listener);
        }
      }).start();
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error finding backup log for " + getUUID(), e);
    }
  }

  private void doRestoreBackup(String url, OBackupFinishedLog finished, String databaseName, OBackupListener listener) {
    ORestoreStartedLog restoreStartedLog = null;
    ODatabaseDocument db = null;
    try {

      db = new ODatabaseDocumentTx(url);

      restoreStartedLog = new ORestoreStartedLog(finished.getUnitId(), logger.nextOpId(), getUUID(), getDbName(),
          finished.getMode());
      logger.log(restoreStartedLog);
      if (!db.exists()) {
        db.create(finished.getPath());
      }
      ORestoreFinishedLog finishedLog = new ORestoreFinishedLog(restoreStartedLog.getUnitId(), restoreStartedLog.getTxId(),
          getUUID(), getDbName(), restoreStartedLog.getMode());

      finishedLog.elapsedTime = finishedLog.getTimestamp().getTime() - restoreStartedLog.getTimestamp().getTime();
      finishedLog.setTargetDB(databaseName);
      finishedLog.path = finished.path;
      finishedLog.restoreUnitId = finished.getUnitId();

      logger.log(finishedLog);

    } catch (Exception e) {

      if (restoreStartedLog != null) {
        ORestoreErrorLog error = new ORestoreErrorLog(restoreStartedLog.getUnitId(), restoreStartedLog.getTxId(), getUUID(),
            getDbName(), getMode().toString());
        error.setMessage(e.getMessage());
        logger.log(error);
        listener.onEvent(cfg, error);
      }
    } finally {
      if (db != null) {
        db.close();
      }
    }
  }

  protected OBackupFinishedLog doBackup(OBackupStartedLog start) {

    ODatabaseDocument db = null;
    try {
      db = getDatabase();
      db.activateOnCurrentThread();
      String path = calculatePath();
      String fName = db.incrementalBackup(path);
      OBackupFinishedLog end = endBackup(start.getUnitId(), start.getTxId());
      end.setFileName(fName);
      end.setPath(path);
      end.fileSize = calculateFileSize(path + File.separator + fName);
      end.elapsedTime = end.getTimestamp().getTime() - start.getTimestamp().getTime();
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

  public abstract Date scheduleNextExecution(OBackupListener listener);

  public OBackupLogger getLogger() {
    return logger;
  }

  protected ODatabaseDocument getDatabase() {

    String dbName = cfg.field(OBackupConfig.DBNAME);

    OServer server = OServerMain.server();

    String url = server.getAvailableStorageNames().get(dbName);

    ODatabaseDocumentInternal db = server.getDatabases().openNoAuthenticate(dbName, null);

    return db;
  }

  public String getUUID() {
    return cfg.field(OBackupConfig.ID);
  }

  public String getDbName() {
    return cfg.field(OBackupConfig.DBNAME);
  }

  public Boolean isEnabled() {
    return cfg.field(OBackupConfig.ENABLED);
  }

  public Integer getRetentionDays() {
    return cfg.field(OBackupConfig.RETENTION_DAYS);
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
      OLogManager.instance().error(this, "Error finding last unfired schedule for UUID : " + getUUID(), e);
    }
    return (OBackupScheduledLog) lastSchedule;
  }

  public void doDeleteBackup(OBackupTask oBackupTask, Long unitId, Long tx) {
    try {
      logger.deleteByUUIDAndUnitIdAndTx(getUUID(), unitId, tx);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error deleting backups for UUID : " + getUUID(), e);
    }
  }

  public void retainLogs() {

    Integer retentionDays = getRetentionDays();
    if (retentionDays != null && retentionDays > 0) {
      retainLogs(retentionDays);
    }
  }

  public void retainLogs(int retentionDays) {

    Calendar c = Calendar.getInstance();
    c.setTime(new Date());
    c.add(Calendar.DATE, (-1) * retentionDays);

    Long time = c.getTime().getTime();
    try {
      logger.deleteByUUIDAndTimestamp(getUUID(), time);
    } catch (IOException e) {
      OLogManager.instance().error(this, "Error deleting backups for UUID : " + getUUID(), e);
    }
  }

  public ODocument getCfg() {
    return cfg;
  }

  public void deleteLastScheduled() {

    OBackupScheduledLog scheduled = lastUnfiredSchedule();
    if (scheduled != null) {
      logger.deleteLog(scheduled);
    }
  }

  @Override
  public boolean equals(Object obj) {
    return this.getClass().isInstance(obj);
  }

  public void markLastBackup() {

    try {
      OBackupFinishedLog lastCompleted = (OBackupFinishedLog) logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID());

      if(lastCompleted!=null) {
        lastCompleted.prevChange = true;
        logger.updateLog(lastCompleted);
      }

    } catch (IOException e) {
      OLogManager.instance().error(this, "Error updating lob backups for UUID : " + getUUID(), e);
    }

  }
}
