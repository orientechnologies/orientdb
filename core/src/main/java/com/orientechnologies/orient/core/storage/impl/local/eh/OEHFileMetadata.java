package com.orientechnologies.orient.core.storage.impl.local.eh;

import com.orientechnologies.orient.core.storage.fs.OFile;

/**
 * @author Andrey Lomakin
 * @since 06.02.13
 */
public class OEHFileMetadata {
  public static final String DEF_EXTENSION     = ".oef";

  private OFile              file;
  private long               bucketsCount;
  private long               tombstonePosition = -1;

  public OFile getFile() {
    return file;
  }

  public void setFile(OFile file) {
    this.file = file;
  }

  public long geBucketsCount() {
    return bucketsCount;
  }

  public void setBucketsCount(long recordsCount) {
    this.bucketsCount = recordsCount;
  }

  public long getTombstonePosition() {
    return tombstonePosition;
  }

  public void setTombstonePosition(long tombstonePosition) {
    this.tombstonePosition = tombstonePosition;
  }
}
