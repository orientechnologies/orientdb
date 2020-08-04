package com.orientechnologies.orient.core.storage.index.versionmap;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;

import java.io.IOException;

public abstract class OVersionPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".vpm";

  public OVersionPositionMap(
      OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }

  // Lifecycle similar to OCluster (e.g. OPaginatedClusterV2)
  public abstract void create(OAtomicOperation atomicOperation);

  public abstract void open() throws IOException;

  public abstract void close();

  public abstract void close(boolean flush) throws IOException;

  public abstract void delete(OAtomicOperation atomicOperation) throws IOException;

  public abstract void synch();
}
