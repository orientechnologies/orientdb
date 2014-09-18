package com.orientechnologies.orient.core.db;

import java.util.concurrent.Callable;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.storage.OStorage;

public interface ODatabaseInternal extends ODatabase {

  /**
   * Returns the underlying storage implementation.
   * 
   * @return The underlying storage implementation
   * @see OStorage
   */
  public OStorage getStorage();

  /**
   * Internal only: replace the storage with a new one.
   * 
   * @param iNewStorage
   *          The new storage to use. Usually it's a wrapped instance of the current cluster.
   */
  public void replaceStorage(OStorage iNewStorage);

  public <V> V callInLock(Callable<V> iCallable, boolean iExclusiveLock);

  public <V> V callInRecordLock(Callable<V> iCallable, ORID rid, boolean iExclusiveLock);

}
