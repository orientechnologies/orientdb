package com.orientechnologies.orient.core.index.hashindex.local.cache;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
class WriteGroup {
  public long[]  pages = new long[16];

  public boolean recencyBit;
  public long    creationTime;

  WriteGroup(long creationTime) {
    this.creationTime = creationTime;

    this.recencyBit = true;
  }
}
