package com.orientechnologies.orient.core.storage.impl.local.paginated;

import java.util.List;

public class OPaginatedClusterDebug {

  public long                    clusterPosition;
  public List<OClusterPageDebug> pages;
  public boolean                 empty;
  public int                     contentSize;
  public long                    fileId;

}
