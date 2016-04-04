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

package com.orientechnologies.agent.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;

/**
 * Created by Enrico Risa on 31/03/16.
 */
public class OBackupLogFactory {

  public OBackupLog fromDoc(ODocument doc) {

    String opString = doc.field(OBackupLog.OP);
    OBackupLogType op = OBackupLogType.valueOf(opString);
    long lsn = doc.field(OBackupLog.LSN);
    String uuid = doc.field(OBackupLog.UUID);
    String dbName = doc.field(OBackupLog.DBNAME);
    String mode = doc.field(OBackupLog.MODE);

    OBackupLog log = null;
    switch (op) {
    case BACKUP_STARTED:
      log = new OBackupStartedLog(lsn, uuid, dbName, mode);
      log.fromDoc(doc);
      break;
    case BACKUP_ERROR:
      log = new OBackupErrorLog(lsn, uuid, dbName, mode);
      log.fromDoc(doc);
      break;
    case BACKUP_FINISHED:
      log = new OBackupFinishedLog(lsn, uuid, dbName, mode);
      log.fromDoc(doc);
      break;
    case BACKUP_SCHEDULED:
      log = new OBackupScheduledLog(lsn, uuid, dbName, mode);
      log.fromDoc(doc);
      break;
    default:
      throw new IllegalStateException("Cannot deserialize passed in log record.");
    }
    return log;
  }
}
