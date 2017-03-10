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

import java.util.Date;

/**
 * Created by Enrico Risa on 25/03/16.
 */
public abstract class OBackupLog {

  public static final String INTERNAL_ID = "internalId";
  public static final String OP          = "op";
  public static final String UNITID      = "unitId";
  public static final String TXID        = "txId";
  public static final String UUID        = "uuid";
  public static final String DBNAME      = "dbName";
  public static final String MODE        = "mode";
  public static final String TIMESTAMP   = "timestamp";
  public static final String TIMESTAMP_UNIX   = "timestampUnix";

  private String internalId;
  private long   unitId;
  private String mode;
  private long   txId;
  private String uuid;
  private String dbName;
  private Date   timestamp;

  public OBackupLog(long unitId, long txId, String uuid, String dbName, String mode) {
    this.txId = txId;
    this.unitId = unitId;
    this.timestamp = new Date();
    this.uuid = uuid;
    this.dbName = dbName;
    this.mode = mode;
  }

  public ODocument toDoc() {
    ODocument log = new ODocument();
    log.field(OP, getType());
    log.field(UNITID, unitId);
    log.field(TXID, txId);
    log.field(TIMESTAMP, timestamp);
    log.field(TIMESTAMP_UNIX, timestamp.getTime());
    log.field(UUID, uuid);
    log.field(MODE, mode);
    log.field(DBNAME, dbName);
    return log;
  }

  public abstract OBackupLogType getType();

  public void fromDoc(ODocument doc) {
    unitId = doc.field(UNITID);
    internalId = doc.getIdentity().toString();
    txId = doc.field(TXID);
    uuid = doc.field(UUID);
    dbName = doc.field(DBNAME);
    timestamp = doc.field(TIMESTAMP);
    mode = doc.field(MODE);
  }

  public long getUnitId() {
    return unitId;
  }

  public long getTxId() {
    return txId;
  }

  public Date getTimestamp() {
    return timestamp;
  }

  public String getUuid() {
    return uuid;
  }

  public String getMode() {
    return mode;
  }

  public String getInternalId() {
    return internalId;
  }
}
