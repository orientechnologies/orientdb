package com.orientechnologies.orient.core.storage.impl.local;

/**
* @author Andrey Lomakin <a href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
* @since 05/02/15
*/
public class OLowDiskSpaceInformation {
public long freeSpace;
public long requiredSpace;

public OLowDiskSpaceInformation(long freeSpace, long requiredSpace) {
this.freeSpace = freeSpace;
this.requiredSpace = requiredSpace;
}
}