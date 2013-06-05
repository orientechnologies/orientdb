package com.orientechnologies.orient.core.index.hashindex.local;

/**
 * @author Andrey Lomakin
 * @since 04.03.13
 */
public class OHashIndexFileLevelMetadata {
  private String fileName;
  private long   bucketsCount;
  private long   tombstoneIndex = -1;

  public OHashIndexFileLevelMetadata(String fileName, long bucketsCount, long tombstoneIndex) {
    this.fileName = fileName;
    this.bucketsCount = bucketsCount;
    this.tombstoneIndex = tombstoneIndex;
  }

  public String getFileName() {
    return fileName;
  }

  public void setFileName(String fileName) {
    this.fileName = fileName;
  }

  public long getBucketsCount() {
    return bucketsCount;
  }

  public void setBucketsCount(long bucketsCount) {
    this.bucketsCount = bucketsCount;
  }

  public long getTombstoneIndex() {
    return tombstoneIndex;
  }

  public void setTombstoneIndex(long tombstoneIndex) {
    this.tombstoneIndex = tombstoneIndex;
  }
}
