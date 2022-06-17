/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * OrientDB management environment, it allow to connect to an environment and manipulate databases
 * or open sessions.
 *
 * <p>Usage example:
 *
 * <p>Remote Example:
 *
 * <pre>
 * <code>
 * OrientDB orientDb = new OrientDB("remote:localhost","root","root");
 * if(orientDb.createIfNotExists("test",ODatabaseType.MEMORY)){
 *  ODatabaseDocument session = orientDb.open("test","admin","admin");
 *  session.createClass("MyClass");
 *  session.close();
 * }
 * ODatabaseDocument session = orientDb.open("test","writer","writer");
 * //...
 * session.close();
 * orientDb.close();
 * </code>
 * </pre>
 *
 * <p>Embedded example:
 *
 * <pre>
 * <code>
 * OrientDB orientDb = new OrientDB("embedded:./databases/",null,null);
 * orientDb.create("test",ODatabaseType.PLOCAL);
 * ODatabaseDocument session = orientDb.open("test","admin","admin");
 * //...
 * session.close();
 * orientDb.close();
 * </code>
 * </pre>
 *
 * <p>Database Manipulation Example:
 *
 * <p>
 *
 * <pre>
 * <code>
 * OrientDB orientDb = ...
 * if(!orientDb.exists("one")){
 *  orientDb.create("one",ODatabaseType.PLOCAL);
 * }
 * if(orientDb.exists("two")){
 *  orientDb.drop("two");
 * }
 * List&ltString&gt databases = orientDb.list();
 * assertEquals(databases.size(),1);
 * assertEquals(databases.get("0"),"one");
 * </code>
 * </pre>
 *
 * <p>
 *
 * <p>
 *
 * <p>Created by tglman on 08/02/17.
 */
public class OrientDB implements AutoCloseable {

  private final ConcurrentLinkedHashMap<ODatabasePoolInternal, ODatabasePool> cachedPools =
      new ConcurrentLinkedHashMap.Builder<ODatabasePoolInternal, ODatabasePool>()
          .maximumWeightedCapacity(100)
          .build(); // cache for links to database pools. Avoid create database pool wrapper each
  // time when it is requested

  protected OrientDBInternal internal;
  protected String serverUser;
  private String serverPassword;

  /**
   * Create a new OrientDb instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("remote:localhost");
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("embedded:./databases/");
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * @param url the url for the specific environment.
   * @param configuration configuration for the specific environment for the list of option {@see
   *     OGlobalConfiguration}.
   */
  public OrientDB(String url, OrientDBConfig configuration) {
    this(url, null, null, configuration);
  }

  /**
   * Create a new OrientDb instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("remote:localhost","root","root");
   * orientDb.create("test",ODatabaseType.PLOCAL);
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OrientDB orientDb = new OrientDB("embedded:./databases/",null,null);
   * orientDb.create("test",ODatabaseType.MEMORY);
   * ODatabaseDocument session = orientDb.open("test","admin","admin");
   * //...
   * session.close();
   * orientDb.close();
   * </code>
   * </pre>
   *
   * @param url the url for the specific environment.
   * @param serverUser the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param configuration configuration for the specific environment for the list of option {@see
   *     OGlobalConfiguration}.
   */
  public OrientDB(
      String url, String serverUser, String serverPassword, OrientDBConfig configuration) {
    int pos;
    String what;
    if ((pos = url.indexOf(':')) > 0) {
      what = url.substring(0, pos);
    } else {
      what = url;
    }
    if ("embedded".equals(what) || "memory".equals(what) || "plocal".equals(what))
      internal = OrientDBInternal.embedded(url.substring(url.indexOf(':') + 1), configuration);
    else if ("remote".equals(what))
      internal =
          OrientDBInternal.remote(url.substring(url.indexOf(':') + 1).split("[,;]"), configuration);
    else throw new IllegalArgumentException("Wrong url:`" + url + "`");

    this.serverUser = serverUser;
    this.serverPassword = serverPassword;
  }

  OrientDB(OrientDBInternal internal) {
    this.internal = internal;
    this.serverUser = null;
    this.serverPassword = null;
  }

  /**
   * Open a database
   *
   * @param database the database to open
   * @param user username of a database user or a server user allowed to open the database
   * @param password related to the specified username
   * @return the opened database
   */
  public ODatabaseSession open(String database, String user, String password) {
    return open(database, user, password, OrientDBConfig.defaultConfig());
  }

  /**
   * Open a database
   *
   * @param database the database to open
   * @param user username of a database user or a server user allowed to open the database
   * @param password related to the specified username
   * @param config custom configuration for current database
   * @return the opened database
   */
  public ODatabaseSession open(
      String database, String user, String password, OrientDBConfig config) {
    return internal.open(database, user, password, config);
  }

  /**
   * Create a new database
   *
   * @param database database name
   * @param type can be plocal or memory
   */
  public void create(String database, ODatabaseType type) {
    create(database, type, OrientDBConfig.defaultConfig());
  }

  /**
   * Create a new database
   *
   * @param database database name
   * @param type can be plocal or memory
   * @param config custom configuration for current database
   */
  public void create(String database, ODatabaseType type, OrientDBConfig config) {
    this.internal.create(database, serverUser, serverPassword, type, config);
  }

  /**
   * Create a new database if not exists
   *
   * @param database database name
   * @param type can be plocal or memory
   * @return true if the database has been created, false if already exists
   */
  public boolean createIfNotExists(String database, ODatabaseType type) {
    return createIfNotExists(database, type, OrientDBConfig.defaultConfig());
  }

  /**
   * Create a new database if not exists
   *
   * @param database database name
   * @param type can be plocal or memory
   * @param config custom configuration for current database
   * @return true if the database has been created, false if already exists
   */
  public boolean createIfNotExists(String database, ODatabaseType type, OrientDBConfig config) {
    if (!this.internal.exists(database, serverUser, serverPassword)) {
      this.internal.create(database, serverUser, serverPassword, type, config);
      return true;
    }
    return false;
  }

  /**
   * Drop a database
   *
   * @param database database name
   */
  public void drop(String database) {
    this.internal.drop(database, serverUser, serverPassword);
  }

  /**
   * Check if a database exists
   *
   * @param database database name to check
   * @return boolean true if exist false otherwise.
   */
  public boolean exists(String database) {
    return this.internal.exists(database, serverUser, serverPassword);
  }

  /**
   * List exiting databases in the current environment
   *
   * @return a list of existing databases.
   */
  public List<String> list() {
    return new ArrayList<>(this.internal.listDatabases(serverUser, serverPassword));
  }

  /** Close the current OrientDB context with all related databases and pools. */
  @Override
  public void close() {
    this.cachedPools.clear();
    this.internal.close();
  }

  /**
   * Check if the current OrientDB context is open
   *
   * @return boolean true if is open false otherwise.
   */
  public boolean isOpen() {
    return this.internal.isOpen();
  }

  ODatabasePoolInternal openPool(
      String database, String user, String password, OrientDBConfig config) {
    return this.internal.openPool(database, user, password, config);
  }

  public ODatabasePool cachedPool(String database, String user, String password) {
    return cachedPool(database, user, password, null);
  }

  /**
   * Retrieve cached database pool with given username and password
   *
   * @param database database name
   * @param user user name
   * @param password user password
   * @param config OrientDB config for pool if need create it (in case if there is no cached pool)
   * @return cached {@link ODatabasePool}
   */
  public ODatabasePool cachedPool(
      String database, String user, String password, OrientDBConfig config) {
    ODatabasePoolInternal internalPool = internal.cachedPool(database, user, password, config);

    ODatabasePool pool = cachedPools.get(internalPool);

    if (pool != null) {
      return pool;
    }

    return cachedPools.computeIfAbsent(internalPool, key -> new ODatabasePool(this, internalPool));
  }

  public void invalidateCachedPools() {
    synchronized (this) {
      cachedPools.forEach((internalPool, pool) -> pool.close());
      cachedPools.clear();
    }
  }

  public OResultSet execute(String script, Map<String, Object> params) {
    return internal.executeServerStatement(script, serverUser, serverPassword, params);
  }

  public OResultSet execute(String script, Object... params) {
    return internal.executeServerStatement(script, serverUser, serverPassword, params);
  }

  OrientDBInternal getInternal() {
    return internal;
  }
}
