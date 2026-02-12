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

package com.orientechnologies.agent.services.backup;

import com.orientechnologies.agent.services.backup.log.OBackupLog;
import com.orientechnologies.agent.services.backup.log.OBackupLogType;
import com.orientechnologies.agent.services.backup.strategy.OBackupStrategy;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.log.OLogger;
import com.orientechnologies.enterprise.server.OEnterpriseServer;
import com.orientechnologies.orient.core.db.OCancellableTimer;
import com.orientechnologies.orient.core.db.OrientDBInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.io.IOException;
import java.util.Date;

/** Created by Enrico Risa on 25/03/16. */
public class OBackupTask implements OBackupListener {
  private static final OLogger logger = OLogManager.instance().logger(OBackupTask.class);
  private OBackupStrategy strategy;
  private OCancellableTimer task;
  private OBackupListener listener;
  private OEnterpriseServer server;
  private int currentRetryCount = 0;

  public OBackupTask(OBackupStrategy strategy, OEnterpriseServer server) {
    this.strategy = strategy;
    this.server = server;
    schedule();
  }

  private void schedule() {
    if (strategy.isEnabled()) {
      final Date nextExecution = strategy.scheduleNextExecution(this);

      OrientDBInternal ctx = server.getDatabases();

      task =
          ctx.delayExecute(
              () -> {
                try {
                  final long start = tickStart();
                  strategy.doBackup(OBackupTask.this, 0);
                  tickEnd(start);
                } catch (final IOException e) {
                  logger.error("Error " + e.getMessage(), e);
                }
              },
              nextExecution);
      logger.info(
          "Scheduled ["
              + strategy.getMode()
              + "] task : "
              + strategy.getUUID()
              + ". Next execution will be "
              + nextExecution);
    }
    strategy.retainLogs();
  }

  private long tickStart() {
    logger.info("Backup started %s ", strategy.getMode());
    return System.currentTimeMillis();
  }

  private void tickEnd(long start) {
    logger.info("Backup %s in (ms): %d", strategy.getMode(), (System.currentTimeMillis() - start));
  }

  public OBackupStrategy getStrategy() {
    return strategy;
  }

  public int getCurrentRetryCount() {
    return currentRetryCount;
  }

  private void resetRetryCount() {
    this.currentRetryCount = 0;
  }

  private long calculateBackoffDelay() {
    switch (currentRetryCount) {
      case 0:
        return 60 * 1000L; // 1 minute
      case 1:
        return 5 * 60 * 1000L; // 5 minutes
      case 2:
        return 15 * 60 * 1000L; // 15 minutes
      default:
        return 30 * 60 * 1000L; // 30 minutes
    }
  }

  public void changeConfig(final OBackupConfig config, final ODocument doc) {
    if (task != null) {
      task.cancel();
    }
    strategy.deleteLastScheduled();
    final OBackupStrategy strategy = config.strategy(doc, this.strategy.getLogger());

    if (!this.strategy.equals(strategy)) {
      strategy.markLastBackup();
    }
    this.strategy = strategy;
    schedule();
  }

  @Override
  public Boolean onEvent(final ODocument cfg, final OBackupLog log) {
    final boolean canContinue = invokeListener(cfg, log);

    if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
      resetRetryCount();
      if (canContinue) {
        schedule();
      }
    } else if (OBackupLogType.BACKUP_ERROR.equals(log.getType())) {
      final int maxRetries = strategy.getRetriesWithDefault();

      if (currentRetryCount < maxRetries) {
        final long delay = calculateBackoffDelay();
        currentRetryCount++;

        logger.warn(
            "Backup failed for ["
                + strategy.getDbName()
                + "]. Retry attempt "
                + currentRetryCount
                + "/"
                + maxRetries
                + " scheduled in "
                + (delay / 1000)
                + " seconds");
        OrientDBInternal ctx = server.getDatabases();

        task =
            ctx.delayExecute(
                () -> {
                  try {
                    final long start = tickStart();
                    strategy.doBackup(OBackupTask.this, currentRetryCount);
                    tickEnd(start);
                  } catch (final IOException e) {
                    logger.error("Error " + e.getMessage(), e);
                  }
                },
                new Date(System.currentTimeMillis() + delay));
      } else {
        logger.warn(
            "Backup failed for ["
                + strategy.getDbName()
                + "] after "
                + maxRetries
                + " retries. Manual intervention required. Backup scheduling stopped.");
        currentRetryCount = 0;
      }
    }

    return true;
  }

  private Boolean invokeListener(ODocument cfg, OBackupLog log) {
    if (listener != null) {
      try {
        return listener.onEvent(cfg, log);
      } catch (Exception e) {
        logger.info("Error invoking listener on event  [%s] ", log.getType());
      }
    }
    return true;
  }

  public void stop() {
    if (task != null) {
      task.cancel();
      logger.info("Cancelled schedule backup on database  [%s] ", strategy.getDbName());
    }
  }

  public void registerListener(final OBackupListener listener) {
    this.listener = listener;
  }

  public void restore(ODocument doc) {
    strategy.doRestore(this, doc);
  }

  public void deleteBackup(final long unitId, final long timestamp) {
    strategy.doDeleteBackup(this, unitId, timestamp);
  }
}
