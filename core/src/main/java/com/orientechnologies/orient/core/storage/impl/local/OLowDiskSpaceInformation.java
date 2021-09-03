package com.orientechnologies.orient.core.storage.impl.local;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a
 *     href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 05/02/15
 */
public class OLowDiskSpaceInformation {
  public final long freeSpace;
  public final long requiredSpace;

  public OLowDiskSpaceInformation(long freeSpace, long requiredSpace) {
    this.freeSpace = freeSpace;
    this.requiredSpace = requiredSpace;
  }
}
