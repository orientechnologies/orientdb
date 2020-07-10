package com.orientechnologies.agent.services.backup.log;

/** Created by Enrico Risa on 18/10/2017. */
public class OBackupUploadErrorLog extends OBackupErrorLog {

  public OBackupUploadErrorLog(long unitId, long txId, String uuid, String dbName, String mode) {
    super(unitId, txId, uuid, dbName, mode);
  }

  @Override
  public OBackupLogType getType() {
    return OBackupLogType.UPLOAD_ERROR;
  }
}
