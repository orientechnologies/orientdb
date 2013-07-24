package com.orientechnologies.orient.core.index.hashindex.local.cache;

/**
 * @author Andrey Lomakin
 * @since 7/24/13
 */
class OWriteGroup {
  public OCacheEntry[] cacheEntries = new OCacheEntry[16];

  public boolean       recencyBit;
  public long          creationTime;

  OWriteGroup(long creationTime) {
    this.creationTime = creationTime;

    this.recencyBit = true;
  }
}
