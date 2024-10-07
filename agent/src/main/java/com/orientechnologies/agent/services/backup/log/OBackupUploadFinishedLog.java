package com.orientechnologies.agent.services.backup.log;

import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.Map;

/** Created by Enrico Risa on 18/10/2017. */
public class OBackupUploadFinishedLog extends OBackupLog {

  protected String uploadType;
  protected String fileName;
  protected long elapsedTime = 0;
  protected long fileSize = 0;
  protected Map<String, String> metadata;

  public OBackupUploadFinishedLog(long unitId, long txId, String uuid, String dbName, String mode) {
    super(unitId, txId, uuid, dbName, mode);
  }

  @Override
  public OBackupLogType getType() {
    return OBackupLogType.UPLOAD_FINISHED;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public String getFileName() {
    return fileName;
  }

  public void setUploadType(String type) {
    this.uploadType = type;
  }

  public String getUploadType() {
    return uploadType;
  }

  public void setElapsedTime(long elapsedTime) {
    this.elapsedTime = elapsedTime;
  }

  public void setMetadata(Map<String, String> metadata) {
    this.metadata = metadata;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }

  public long getFileSize() {
    return fileSize;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public void setFileSize(long fileSize) {
    this.fileSize = fileSize;
  }

  @Override
  public ODocument toDoc() {
    ODocument document = super.toDoc();
    document.field("metadata", this.metadata);
    document.field("uploadType", this.uploadType);
    document.field("elapsedTime", this.elapsedTime);
    document.field("fileSize", this.fileSize);
    document.field("fileName", this.fileName);

    return document;
  }

  @Override
  public void fromDoc(ODocument doc) {
    super.fromDoc(doc);
    this.metadata = doc.field("metadata");
    this.uploadType = doc.field("uploadType");
    this.elapsedTime = doc.field("elapsedTime");
    this.fileSize = doc.field("fileSize");
    this.fileName = doc.field("fileName");
  }
}
