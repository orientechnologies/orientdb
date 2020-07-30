package com.orientechnologies.orient.core.storage.index.versionmap;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

public abstract class OVersionPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".vpm";

  public OVersionPositionMap(
      OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }
}
