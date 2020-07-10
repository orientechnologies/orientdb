package com.orientechnologies.backup.uploader;

import java.util.Map;

/** Created by Enrico Risa on 23/10/2017. */
public class OUploadMetadata {

  protected String type;
  protected long elapsedTime = 0;
  private Map<String, String> metadata;

  public OUploadMetadata(String type, long elapsedTime, Map<String, String> metadata) {
    this.type = type;
    this.elapsedTime = elapsedTime;
    this.metadata = metadata;
  }

  public Map<String, String> getMetadata() {
    return metadata;
  }

  public String getType() {
    return type;
  }

  public long getElapsedTime() {
    return elapsedTime;
  }
}
