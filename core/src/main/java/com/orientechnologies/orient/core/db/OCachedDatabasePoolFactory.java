package com.orientechnologies.orient.core.db;

/**
 * Cached database pool factory which allows store database pools associated with users
 *
 * @author Vitalii Honchar (weaxme@gmail.com)
 */
public interface OCachedDatabasePoolFactory {

  /**
   * Get {@link ODatabasePoolInternal} from cache or create and cache new {@link
   * ODatabasePoolInternal}
   *
   * @param database name of database
   * @param username user name
   * @param password user password
   * @return {@link ODatabasePoolInternal} cached database pool
   */
  ODatabasePoolInternal get(
      String database, String username, String password, OrientDBConfig config);

  /**
   * Close all cached pools and clear cache
   *
   * @return this instance
   */
  OCachedDatabasePoolFactory reset();

  /** Close all cached pools, clear cache. Can't use this factory after close. */
  void close();

  /**
   * Check if factory is closed
   *
   * @return true if factory is closed
   */
  boolean isClosed();
}
