package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.exception.OAcquireTimeoutException;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;

/**
 * A Pool of databases.
 *
 * <p>Example of usage with an OrientDB context:
 *
 * <p>
 *
 * <pre>
 * <code>
 * OrientDB orientDb= new OrientDB("remote:localhost","root","password");
 * //...
 * ODatabasePool pool = new ODatabasePool(orientDb,"myDb","admin","adminpwd");
 * ODatabaseDocument session = pool.acquire();
 * //....
 * session.close();
 * pool.close();
 * orientDb.close();
 *
 * </code>
 * </pre>
 *
 * <p>
 *
 * <p>
 *
 * <p>Example of usage as simple access to a specific database without a context:
 *
 * <p>
 *
 * <pre><code>
 * ODatabasePool pool = new ODatabasePool("remote:localhost/myDb","admin","adminpwd");
 * ODatabaseDocument session = pool.acquire();
 * //....
 * session.close();
 * pool.close();
 *
 * </code></pre>
 *
 * <p>
 *
 * <p>Created by tglman on 08/02/17.
 */
public class ODatabasePool implements AutoCloseable {

  private final OrientDB orientDb;
  private ODatabasePoolInternal internal;
  private final boolean autoclose;

  /**
   * Open a new database pool on a specific environment.
   *
   * @param environment the starting environment.
   * @param database the database name
   * @param user the database user for the current pool of databases.
   * @param password the password relative to the user name
   */
  public ODatabasePool(OrientDB environment, String database, String user, String password) {
    this(environment, database, user, password, OrientDBConfig.defaultConfig());
  }

  /**
   * Open a new database pool on a specific environment, with a specific configuration for this
   * pool.
   *
   * @param environment the starting environment.
   * @param database the database name
   * @param user the database user for the current pool of databases.
   * @param password the password relative to the user name
   * @param configuration the configuration relative for the current pool.
   */
  public ODatabasePool(
      OrientDB environment,
      String database,
      String user,
      String password,
      OrientDBConfig configuration) {
    orientDb = environment;
    autoclose = false;
    internal = orientDb.openPool(database, user, password, configuration);
  }

  /**
   * Open a new database pool from a url, useful in case the application access to only a database
   * or do not manipulate databases.
   *
   * @param url the full url for a database, like "embedded:/full/path/to/database" or
   *     "remote:localhost/database"
   * @param user the database user for the current pool of databases.
   * @param password the password relative to the user
   */
  public ODatabasePool(String url, String user, String password) {
    this(url, user, password, OrientDBConfig.defaultConfig());
  }

  /**
   * Open a new database pool from a url and additional configuration, useful in case the
   * application access to only a database or do not manipulate databases.
   *
   * @param url the full url for a database, like "embedded:/full/path/to/database" or
   *     "remote:localhost/database"
   * @param user the database user for the current pool of databases.
   * @param password the password relative to the user
   * @param configuration the configuration relative to the current pool.
   */
  public ODatabasePool(String url, String user, String password, OrientDBConfig configuration) {
    OURLConnection val = OURLHelper.parseNew(url);
    orientDb = new OrientDB(val.getType() + ":" + val.getPath(), configuration);
    autoclose = true;
    internal = orientDb.openPool(val.getDbName(), user, password, configuration);
  }

  /**
   * Open a new database pool from a environment and a database name, useful in case the application
   * access to only a database or do not manipulate databases.
   *
   * @param environment the url for an environemnt, like "embedded:/the/environment/path/" or
   *     "remote:localhost"
   * @param database the database for the current url.
   * @param user the database user for the current pool of databases.
   * @param password the password relative to the user
   */
  public ODatabasePool(String environment, String database, String user, String password) {
    this(environment, database, user, password, OrientDBConfig.defaultConfig());
  }

  /**
   * Open a new database pool from a environment and a database name with a custom configuration,
   * useful in case the application access to only a database or do not manipulate databases.
   *
   * @param environment the url for an environemnt, like "embedded:/the/environment/path/" or
   *     "remote:localhost"
   * @param database the database for the current url.
   * @param user the database user for the current pool of databases.
   * @param password the password relative to the user
   * @param configuration the configuration relative to the current pool.
   */
  public ODatabasePool(
      String environment,
      String database,
      String user,
      String password,
      OrientDBConfig configuration) {
    orientDb = new OrientDB(environment, configuration);
    autoclose = true;
    internal = orientDb.openPool(database, user, password, configuration);
  }

  ODatabasePool(OrientDB environment, ODatabasePoolInternal internal) {
    this.orientDb = environment;
    this.internal = internal;
    autoclose = false;
  }

  /**
   * Acquire a session from the pool, if no session are available will wait until a session is
   * available or a timeout is reached
   *
   * @return a session from the pool.
   * @throws OAcquireTimeoutException in case the timeout for waiting for a session is reached.
   */
  public ODatabaseSession acquire() throws OAcquireTimeoutException {
    return internal.acquire();
  }

  @Override
  public void close() {
    internal.close();
    if (autoclose) orientDb.close();
  }

  /**
   * Check if database pool is closed
   *
   * @return true if database pool is closed
   */
  public boolean isClosed() {
    return internal.isClosed();
  }
}
