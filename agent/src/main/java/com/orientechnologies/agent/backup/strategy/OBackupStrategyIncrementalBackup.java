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
import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.agent.backup.log.OBackupLogger;
import com.orientechnologies.agent.backup.log.OBackupScheduledLog;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OCronExpression;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;

import java.io.File;
import java.text.ParseException;
import java.util.Date;

/**
 * Created by Enrico Risa on 25/03/16.
 */
public class OBackupStrategyIncrementalBackup extends OBackupStrategy {

  public OBackupStrategyIncrementalBackup(ODocument cfg, OBackupLogger logger) {
    super(cfg, logger);
  }

  @Override
  public OAutomaticBackup.MODE getMode() {
    return OAutomaticBackup.MODE.INCREMENTAL_BACKUP;
  }

  protected String calculatePath() {

    String basePath = cfg.field(OBackupConfig.DIRECTORY);
    String dbName = cfg.field(OBackupConfig.DBNAME);
    return basePath + File.separator + dbName + "-incremental";

  }

  @Override
  public Date scheduleNextExecution(OBackupListener listener) {

    OBackupScheduledLog last = lastUnfiredSchedule();

    if (last == null) {
      ODocument full = (ODocument) cfg.eval(OBackupConfig.MODES + "." + OAutomaticBackup.MODE.INCREMENTAL_BACKUP);
      String when = full.field(OBackupConfig.WHEN);
      try {
        OCronExpression expression = new OCronExpression(when);
        Date nextExecution = expression.getNextValidTimeAfter(new Date());
        Long unitId = logger.nextOpId();
        try {
          OBackupLog lastCompleted = logger.findLast(OBackupLogType.BACKUP_FINISHED, getUUID());

          if (lastCompleted != null) {
            unitId = lastCompleted.getUnitId();
          }

        } catch (Exception e) {
          e.printStackTrace();
        }
        OBackupScheduledLog log = new OBackupScheduledLog(unitId, logger.nextOpId(), getUUID(), getDbName(), getMode().toString());
        log.nextExecution = nextExecution.getTime();
        getLogger().log(log);
        listener.onEvent(cfg, log);
        return nextExecution;
      } catch (ParseException e) {
        e.printStackTrace();
      }

    } else {
      return new Date(last.nextExecution);
    }

    return null;
  }
}
