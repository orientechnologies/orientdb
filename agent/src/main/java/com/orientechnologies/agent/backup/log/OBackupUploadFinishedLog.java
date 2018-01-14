package com.orientechnologies.agent.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.Map;

/**
 * Created by Enrico Risa on 18/10/2017.
 */
public class OBackupUploadFinishedLog extends OBackupLog {

  protected String type;
  protected long elapsedTime = 0;
  protected long fileSize    = 0;
  protected Map<String, String> metadata;

  public OBackupUploadFinishedLog(long unitId, long txId, String uuid, String dbName, String mode) {
    super(unitId, txId, uuid, dbName, mode);
  }

  @Override
  public OBackupLogType getType() {
    return OBackupLogType.UPLOAD_FINISHED;
  }

  public void setType(String type) {
    this.type = type;
  }

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  @Override
  public ODocument toDoc() {
    ODocument document = super.toDoc();
    document.field("metadata", this.metadata);
    document.field("type", this.type);
    document.field("elapsedTime", this.elapsedTime);
    document.field("fileSize", this.fileSize);

    return document;
  }

  @Override
  public void fromDoc(ODocument doc) {
    super.fromDoc(doc);
    this.metadata = doc.field("metadata");
    this.type = doc.field("type");
    this.elapsedTime = doc.field("elapsedTime");
    this.fileSize = doc.field("fileSize");
  }
}
