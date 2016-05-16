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
import com.orientechnologies.agent.backup.log.*;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OCronExpression;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Enrico Risa on 25/03/16.
 */
public class OBackupStrategyMixBackup extends OBackupStrategy {

  protected boolean isIncremental = false;

  public OBackupStrategyMixBackup(ODocument cfg, OBackupLogger logger) {
    super(cfg, logger);
  }

  @Override
  public OAutomaticBackup.MODE getMode() {
    return isIncremental ? OAutomaticBackup.MODE.INCREMENTAL_BACKUP : OAutomaticBackup.MODE.FULL_BACKUP;
  }

  protected String calculatePath() {
    if (!isIncremental) {
      return defaultPath();
    }
    OBackupFinishedLog last = null;
    try {
      last = (OBackupFinishedLog) logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID());
    } catch (Exception e) {
      e.printStackTrace();
    }
    return last != null ? last.getPath() : defaultPath();

  }

  protected String defaultPath() {

    long begin = System.currentTimeMillis();
    try {
      OBackupLog last = logger.findLast(OBackupLogType.BACKUP_SCHEDULED, getUUID());
      if (last != null) {
        begin = last.getUnitId();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
    String basePath = cfg.field(OBackupConfig.DIRECTORY);
    String dbName = cfg.field(OBackupConfig.DBNAME);
    return basePath + File.separator + dbName + "-" + begin;
  }

  @Override
  public Date scheduleNextExecution() {

    OBackupScheduledLog last = lastUnfiredSchedule();

    if (last == null) {
      ODocument full = (ODocument) cfg.eval(OBackupConfig.MODES + "." + OAutomaticBackup.MODE.FULL_BACKUP);
      String whenFull = full.field(OBackupConfig.WHEN);
      ODocument incremental = (ODocument) cfg.eval(OBackupConfig.MODES + "." + OAutomaticBackup.MODE.INCREMENTAL_BACKUP);
      String whenIncremental = incremental.field(OBackupConfig.WHEN);
      try {
        OCronExpression eFull = new OCronExpression(whenFull);
        OCronExpression eIncremental = new OCronExpression(whenIncremental);
        Date now = new Date();

        Date nextFull = eFull.getNextValidTimeAfter(now);
        Date nextIncremental = eIncremental.getNextValidTimeAfter(now);
        OBackupLog lastCompleted = null;
        Long unitId = logger.nextOpId();
        try {
          lastCompleted = logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID());
          if (lastCompleted != null && nextIncremental.before(nextFull)) {
            unitId = lastCompleted.getUnitId();
            isIncremental = true;
          } else {
            isIncremental = false;
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        Date nextExecution = nextIncremental.before(nextFull) ? nextIncremental : nextFull;
        OBackupScheduledLog log = new OBackupScheduledLog(unitId, logger.nextOpId(), getUUID(), getDbName(), getMode().toString());
        log.nextExecution = nextExecution.getTime();
        getLogger().log(log);
        return nextExecution;
      } catch (ParseException e) {
        e.printStackTrace();
      }
      return null;
    } else {
      isIncremental = OAutomaticBackup.MODE.INCREMENTAL_BACKUP.toString().equalsIgnoreCase(last.getMode());
      return new Date(last.nextExecution);
    }
  }

}
