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

import com.orientechnologies.agent.backup.log.OBackupLog;
import com.orientechnologies.agent.backup.log.OBackupLogType;
import com.orientechnologies.agent.backup.strategy.OBackupStrategy;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.io.IOException;
import java.util.Date;
import java.util.TimerTask;

/**
 * Created by Enrico Risa on 25/03/16.
 */
public class OBackupTask implements OBackupListener {

  private OBackupStrategy strategy;
  private TimerTask       task;

  public OBackupTask(OBackupStrategy strategy) {
    this.strategy = strategy;
    schedule();
  }

  protected void schedule() {

    if (strategy.isEnabled()) {
      Date nextExecution = strategy.scheduleNextExecution();

      task = new TimerTask() {
        @Override
        public void run() {
          try {
            strategy.doBackup(OBackupTask.this);
          } catch (IOException e) {
            e.printStackTrace();
          }
        }
      };
      Orient.instance().scheduleTask(task, nextExecution, 0);

      OLogManager.instance().info(this,
          "Scheduled [" + strategy.getMode() + "] task : " + strategy.getUUID() + ". Next execution will be " + nextExecution);
    }


    strategy.retainLogs();

  }

  public void changeConfig(OBackupConfig config, ODocument doc) {
    if (task != null) {
      task.cancel();
    }
    strategy = config.strategy(doc, strategy.getLogger());

    schedule();
  }

  @Override
  public void onEvent(ODocument cfg, OBackupLog log) {

    if (OBackupLogType.BACKUP_FINISHED.equals(log.getType())) {
      schedule();
    }
  }

  public void stop() {
    if (task != null) {
      task.cancel();
      OLogManager.instance().info(this, "Cancelled schedule backup on database  [" + strategy.getDbName() + "] ");
    }
  }

  public void restore(ODocument doc) {
    strategy.doRestore(this, doc);
  }

  public void deleteBackup(Long unitId, Long timestamp) {
    strategy.doDeleteBackup(this, unitId, timestamp);
  }
}