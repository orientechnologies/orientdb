package com.orientechnologies.orient.core.storage.index.versionmap;

import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.impl.local.paginated.atomicoperations.OAtomicOperation;
import com.orientechnologies.orient.core.storage.impl.local.paginated.base.ODurableComponent;
import java.io.IOException;

public abstract class OVersionPositionMap extends ODurableComponent {
  public static final String DEF_EXTENSION = ".vpm";

  public static final int MAX_CONCURRENT_DISTRIBUTED_TRANSACTIONS = 1000;
  public static final int MAGIC_SAFETY_FILL_FACTOR = 10;
  public static final int DEFAULT_VERSION_ARRAY_SIZE =
      MAX_CONCURRENT_DISTRIBUTED_TRANSACTIONS * MAGIC_SAFETY_FILL_FACTOR;

  public OVersionPositionMap(
      OAbstractPaginatedStorage storage, String name, String extension, String lockName) {
    super(storage, name, extension, lockName);
  }

  // Lifecycle similar to OCluster (e.g. OPaginatedClusterV2)
  public abstract void create(OAtomicOperation atomicOperation);

  public abstract void open() throws IOException;

  public abstract void delete(OAtomicOperation atomicOperation) throws IOException;

  // VPM only stores an array of type integer for versions
  public abstract void updateVersion(int versionHash);

  public abstract int getVersion(int versionHash);

  public abstract int getKeyHash(Object key);
}
