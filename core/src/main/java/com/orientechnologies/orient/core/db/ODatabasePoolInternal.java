package com.orientechnologies.orient.core.db;

/** Created by tglman on 27/06/16. */
public interface ODatabasePoolInternal extends AutoCloseable {

  ODatabaseSession acquire();

  void close();

  void release(ODatabaseDocumentInternal database);

  OrientDBConfig getConfig();

  /**
   * Check if database pool is closed
   *
   * @return true if pool is closed
   */
  boolean isClosed();

  /**
   * Check that all resources owned by the pool are in the pool
   *
   * @return
   */
  boolean isUnused();

  /**
   * Check last time that a resource was returned to the pool
   *
   * @return
   */
  long getLastCloseTime();
}
