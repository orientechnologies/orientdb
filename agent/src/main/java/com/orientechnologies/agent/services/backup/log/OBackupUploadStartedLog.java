package com.orientechnologies.agent.services.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;

/** Created by Enrico Risa on 18/10/2017. */
public class OBackupUploadStartedLog extends OBackupLog {

  public String fileName;
  public String path;

  public OBackupUploadStartedLog(long unitId, long txId, String uuid, String dbName, String mode) {
    super(unitId, txId, uuid, dbName, mode);
  }

  @Override
  public OBackupLogType getType() {
    return OBackupLogType.UPLOAD_STARTED;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  @Override
  public ODocument toDoc() {
    ODocument document = super.toDoc();
    document.field("fileName", this.fileName);
    document.field("path", this.path);
    return document;
  }

  @Override
  public void fromDoc(ODocument doc) {
    super.fromDoc(doc);
    this.fileName = doc.field("fileName");
    this.path = doc.field("path");
  }

  public void setPath(String path) {
    this.path = path;
  }
}
