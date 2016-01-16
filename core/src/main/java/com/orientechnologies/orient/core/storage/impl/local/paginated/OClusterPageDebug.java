package com.orientechnologies.orient.core.storage.impl.local.paginated;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

@SuppressFBWarnings("URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD")
public class OClusterPageDebug {
  public long   pageIndex      = -1;
  public int    inPagePosition = -1;
  public int    inPageSize     = -1;
  public byte[] content;

}
