package com.orientechnologies.orient.core.index.hashindex.local;

import com.orientechnologies.orient.core.config.OStorageSegmentConfiguration;

/**
 * @author Andrey Lomakin
 * @since 04.03.13
 */
public class OHashIndexFileLevelMetadata {
  private OStorageSegmentConfiguration fileConfiguration;
  private long                         bucketsCount;
  private long                         tombstoneIndex = -1;

  public OHashIndexFileLevelMetadata(OStorageSegmentConfiguration fileConfiguration, long bucketsCount, long tombstoneIndex) {
    this.fileConfiguration = fileConfiguration;
    this.bucketsCount = bucketsCount;
    this.tombstoneIndex = tombstoneIndex;
  }

  public OStorageSegmentConfiguration getFileConfiguration() {
    return fileConfiguration;
  }

  public void setFileConfiguration(OStorageSegmentConfiguration fileConfiguration) {
    this.fileConfiguration = fileConfiguration;
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
