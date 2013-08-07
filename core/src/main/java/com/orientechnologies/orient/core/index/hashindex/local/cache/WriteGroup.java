package com.orientechnologies.orient.core.index.hashindex.local.cache;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
class WriteGroup {
  public OCachePointer[]  pages = new OCachePointer[16];

  public volatile boolean recencyBit;
  public final long       creationTime;

  WriteGroup(long creationTime) {
    this.recencyBit = true;
    this.creationTime = creationTime;
  }
}
