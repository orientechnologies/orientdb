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
 * Created by Enrico Risa on 25/03/16.
 */
public abstract class OBackupLog {

  public static final String OP        = "op";
  public static final String LSN       = "lsn";
  public static final String UUID      = "uuid";
  public static final String DBNAME    = "dbName";
  public static final String MODE      = "mode";
  public static final String TIMESTAMP = "timestamp";

  private String             mode;
  private long               lsn;
  private String             uuid;
  private String             dbName;
  private long               timestamp;

  public OBackupLog(long opsId, String uuid, String dbName, String mode) {
    this.lsn = opsId;
    this.timestamp = System.currentTimeMillis() / 1000L;
    this.uuid = uuid;
    this.dbName = dbName;
    this.mode = mode;
  }

  public ODocument toDoc() {
    ODocument log = new ODocument();
    log.field(OP, getType());
    log.field(LSN, lsn);
    log.field(TIMESTAMP, timestamp);
    log.field(UUID, uuid);
    log.field(MODE, mode);
    log.field(DBNAME, dbName);
    return log;
  }

  public abstract OBackupLogType getType();

  public void fromDoc(ODocument doc) {
    lsn = doc.field(LSN);
    uuid = doc.field(UUID);
    dbName = doc.field(DBNAME);
    timestamp = doc.field(TIMESTAMP);
    mode = doc.field(MODE);
  }

  public long getLsn() {
    return lsn;
  }

  public long getTimestamp() {
    return timestamp;
  }

  public String getUuid() {
    return uuid;
  }

  public String getMode() {
    return mode;
  }
}
