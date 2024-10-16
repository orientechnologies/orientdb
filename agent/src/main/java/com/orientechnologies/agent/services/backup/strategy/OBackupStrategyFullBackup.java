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

package com.orientechnologies.agent.services.backup.strategy;

import com.orientechnologies.agent.services.backup.OBackupConfig;
import com.orientechnologies.agent.services.backup.OBackupListener;
import com.orientechnologies.agent.services.backup.log.OBackupLogger;
import com.orientechnologies.agent.services.backup.log.OBackupScheduledLog;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.schedule.OCronExpression;
import com.orientechnologies.orient.server.handler.OAutomaticBackup;
import java.io.File;
import java.text.ParseException;
import java.util.Date;

/** Created by Enrico Risa on 25/03/16. */
public class OBackupStrategyFullBackup extends OBackupStrategy {
  private static final OLogger log = OLogManager.instance().logger(OBackupStrategyFullBackup.class);

  public OBackupStrategyFullBackup(final ODocument cfg, final OBackupLogger logger) {
    super(cfg, logger);
  }

  @Override
  public OAutomaticBackup.MODE getMode() {
    return OAutomaticBackup.MODE.FULL_BACKUP;
  }

  protected String calculatePath(ODatabaseDocument db) {
    final long begin = System.currentTimeMillis();
    final String basePath = cfg.field(OBackupConfig.DIRECTORY);
    final String dbName = cfg.field(OBackupConfig.DBNAME);
    return basePath + File.separator + dbName + "-" + begin;
  }

  @Override
  public Date scheduleNextExecution(final OBackupListener listener) {
    final OBackupScheduledLog last = lastUnfiredSchedule();

    if (last == null) {
      final ODocument full =
          (ODocument) cfg.eval(OBackupConfig.MODES + "." + OAutomaticBackup.MODE.FULL_BACKUP);
      final String when = full.field(OBackupConfig.WHEN);
      try {
        final OCronExpression expression = new OCronExpression(when);
        final Date nextExecution = expression.getNextValidTimeAfter(new Date());
        final OBackupScheduledLog log =
            new OBackupScheduledLog(
                logger.nextOpId(), logger.nextOpId(), getUUID(), getDbName(), getMode().toString());
        log.nextExecution = nextExecution.getTime();
        getLogger().log(log);
        listener.onEvent(cfg, log);
        return nextExecution;
      } catch (ParseException e) {
        log.warn("Parse exception: %s", e, e.getMessage());
      }
    } else {
      return new Date(last.nextExecution);
    }
    return null;
  }
}
