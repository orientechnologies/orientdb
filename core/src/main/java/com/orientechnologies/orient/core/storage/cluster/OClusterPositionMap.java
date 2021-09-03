package com.orientechnologies.orient.core.storage.cluster;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

public abstract class OClusterPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".cpm";

  public OClusterPositionMap(
      OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }
}
