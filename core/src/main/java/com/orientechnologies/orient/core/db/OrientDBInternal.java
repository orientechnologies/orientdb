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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandOutputListener;
import com.orientechnologies.orient.core.command.script.OScriptManager;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.metadata.security.auth.OAuthenticationInfo;
import com.orientechnologies.orient.core.security.OSecuritySystem;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.OStorage;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

/** Created by tglman on 27/03/16. */
public interface OrientDBInternal extends AutoCloseable, OSchedulerInternal {

  /**
   * Create a new factory from a given url.
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * @param url the url for the specific factory.
   * @param configuration configuration for the specific factory for the list of option {@see
   *     OGlobalConfiguration}.
   * @return the new Orient Factory.
   */
  static OrientDBInternal fromUrl(String url, OrientDBConfig configuration) {
    String what = url.substring(0, url.indexOf(':'));
    if ("embedded".equals(what))
      return embedded(url.substring(url.indexOf(':') + 1), configuration);
    else if ("remote".equals(what))
      return remote(url.substring(url.indexOf(':') + 1).split(";"), configuration);
    throw new ODatabaseException("not supported database type");
  }

  default OrientDB newOrientDB() {
    return new OrientDB(this);
  }

  default OrientDB newOrientDBNoClose() {
    return new OrientDB(this) {
      @Override
      public void close() {}
    };
  }
  /**
   * Create a new remote factory
   *
   * @param hosts array of hosts
   * @param configuration configuration for the specific factory for the list of option {@see
   *     OGlobalConfiguration}.
   * @return a new remote databases factory
   */
  static OrientDBInternal remote(String[] hosts, OrientDBConfig configuration) {
    OrientDBInternal factory;

    try {
      String className = "com.orientechnologies.orient.core.db.OrientDBRemote";
      ClassLoader loader;
      if (configuration != null) {
        loader = configuration.getClassLoader();
      } else {
        loader = OrientDBInternal.class.getClassLoader();
      }
      Class<?> kass = loader.loadClass(className);
      Constructor<?> constructor =
          kass.getConstructor(String[].class, OrientDBConfig.class, Orient.class);
      factory = (OrientDBInternal) constructor.newInstance(hosts, configuration, Orient.instance());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InstantiationException e) {
      throw OException.wrapException(new ODatabaseException("OrientDB client API missing"), e);
    } catch (InvocationTargetException e) {
      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw OException.wrapException(
          new ODatabaseException("Error creating OrientDB remote factory"), e.getTargetException());
    }
    return factory;
  }

  /**
   * Create a new Embedded factory
   *
   * @param directoryPath base path where the database are hosted
   * @param config configuration for the specific factory for the list of option {@see
   *     OGlobalConfiguration}
   * @return a new embedded databases factory
   */
  static OrientDBInternal embedded(String directoryPath, OrientDBConfig config) {
    return new OrientDBEmbedded(directoryPath, config, Orient.instance());
  }

  static OrientDBInternal distributed(String directoryPath, OrientDBConfig configuration) {
    OrientDBInternal factory;

    try {
      ClassLoader loader;
      if (configuration != null) {
        loader = configuration.getClassLoader();
      } else {
        loader = OrientDBInternal.class.getClassLoader();
      }
      Class<?> kass;
      try {
        String className = "com.orientechnologies.orient.core.db.OrientDBDistributed";
        kass = loader.loadClass(className);
      } catch (ClassNotFoundException e) {
        String className = "com.orientechnologies.orient.distributed.OrientDBDistributed";
        kass = loader.loadClass(className);
      }
      Constructor<?> constructor =
          kass.getConstructor(String.class, OrientDBConfig.class, Orient.class);
      factory =
          (OrientDBInternal)
              constructor.newInstance(directoryPath, configuration, Orient.instance());
    } catch (ClassNotFoundException
        | NoSuchMethodException
        | IllegalAccessException
        | InstantiationException e) {
      throw OException.wrapException(new ODatabaseException("OrientDB distributed API missing"), e);
    } catch (InvocationTargetException e) {
      //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
      throw OException.wrapException(
          new ODatabaseException("Error creating OrientDB remote factory"), e.getTargetException());
    }
    return factory;
  }

  /**
   * Open a database specified by name using the username and password if needed
   *
   * @param name of the database to open
   * @param user the username allowed to open the database
   * @param password related to the specified username
   * @return the opened database
   */
  ODatabaseDocumentInternal open(String name, String user, String password);

  /**
   * Open a database specified by name using the username and password if needed, with specific
   * configuration
   *
   * @param name of the database to open
   * @param user the username allowed to open the database
   * @param password related to the specified username
   * @param config database specific configuration that override the factory global settings where
   *     needed.
   * @return the opened database
   */
  ODatabaseDocumentInternal open(String name, String user, String password, OrientDBConfig config);

  /**
   * Open a database specified by name using the authentication info provided, with specific
   * configuration
   *
   * @param authenticationInfo authentication informations provided for the authentication.
   * @param config database specific configuration that override the factory global settings where
   *     needed.
   * @return the opened database
   */
  ODatabaseDocumentInternal open(OAuthenticationInfo authenticationInfo, OrientDBConfig config);

  /**
   * Create a new database
   *
   * @param name database name
   * @param user the username of a user allowed to create a database, in case of remote is a server
   *     user for embedded it can be left empty
   * @param password the password relative to the user
   * @param type can be plocal or memory
   */
  void create(String name, String user, String password, ODatabaseType type);

  /**
   * Create a new database
   *
   * @param name database name
   * @param user the username of a user allowed to create a database, in case of remote is a server
   *     user for embedded it can be left empty
   * @param password the password relative to the user
   * @param config database specific configuration that override the factory global settings where
   *     needed.
   * @param type can be plocal or memory
   */
  void create(String name, String user, String password, ODatabaseType type, OrientDBConfig config);

  /**
   * Check if a database exists
   *
   * @param name database name to check
   * @param user the username of a user allowed to check the database existence, in case of remote
   *     is a server user for embedded it can be left empty.
   * @param password the password relative to the user
   * @return boolean true if exist false otherwise.
   */
  boolean exists(String name, String user, String password);

  /**
   * Drop a database
   *
   * @param name database name
   * @param user the username of a user allowed to drop a database, in case of remote is a server
   *     user for embedded it can be left empty
   * @param password the password relative to the user
   */
  void drop(String name, String user, String password);

  /**
   * List of database exiting in the current environment
   *
   * @param user the username of a user allowed to list databases, in case of remote is a server
   *     user for embedded it can be left empty
   * @param password the password relative to the user
   * @return a set of databases names.
   */
  Set<String> listDatabases(String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name database name
   * @param user the username allowed to open the database
   * @param password the password relative to the user
   * @return a new pool of databases.
   */
  ODatabasePoolInternal openPool(String name, String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name database name
   * @param user the username allowed to open the database
   * @param password the password relative to the user
   * @param config database specific configuration that override the factory global settings where
   *     needed.
   * @return a new pool of databases.
   */
  ODatabasePoolInternal openPool(String name, String user, String password, OrientDBConfig config);

  ODatabasePoolInternal cachedPool(String database, String user, String password);

  ODatabasePoolInternal cachedPool(
      String database, String user, String password, OrientDBConfig config);

  /** Internal api for request to open a database with a pool */
  ODatabaseDocumentInternal poolOpen(
      String name, String user, String password, ODatabasePoolInternal pool);

  void restore(
      String name,
      String user,
      String password,
      ODatabaseType type,
      String path,
      OrientDBConfig config);

  void restore(
      String name,
      InputStream in,
      Map<String, Object> options,
      Callable<Object> callable,
      OCommandOutputListener iListener);

  /** Close the factory with all related databases and pools. */
  void close();

  /** Should be called only by shutdown listeners */
  void internalClose();

  /** Internal API for pool close */
  void removePool(ODatabasePoolInternal toRemove);

  /** Check if the current instance is open */
  boolean isOpen();

  boolean isEmbedded();

  default boolean isMemoryOnly() {
    return false;
  }

  static OrientDBInternal extract(OrientDB orientDB) {
    return orientDB.internal;
  }

  static String extractUser(OrientDB orientDB) {
    return orientDB.serverUser;
  }

  ODatabaseDocumentInternal openNoAuthenticate(String iDbUrl, String user);

  ODatabaseDocumentInternal openNoAuthorization(String name);

  void initCustomStorage(String name, String baseUrl, String userName, String userPassword);

  void loadAllDatabases();

  void removeShutdownHook();

  Collection<OStorage> getStorages();

  void forceDatabaseClose(String databaseName);

  <X> Future<X> execute(String database, String user, ODatabaseTask<X> task);

  <X> Future<X> executeNoAuthorization(String database, ODatabaseTask<X> task);

  default OStorage fullSync(String dbName, InputStream backupStream, OrientDBConfig config) {
    throw new UnsupportedOperationException();
  }

  default OScriptManager getScriptManager() {
    throw new UnsupportedOperationException();
  }

  default void networkRestore(String databaseName, InputStream in, Callable<Object> callback) {
    throw new UnsupportedOperationException();
  }

  default OResultSet executeServerStatement(
      String script, String user, String pw, Map<String, Object> params) {
    throw new UnsupportedOperationException();
  }

  default OResultSet executeServerStatement(
      String script, String user, String pw, Object... params) {
    throw new UnsupportedOperationException();
  }

  default OSystemDatabase getSystemDatabase() {
    throw new UnsupportedOperationException();
  }

  default String getBasePath() {
    throw new UnsupportedOperationException();
  }

  void internalDrop(String database);

  void create(
      String name,
      String user,
      String password,
      ODatabaseType type,
      OrientDBConfig config,
      ODatabaseTask<Void> createOps);

  OrientDBConfig getConfigurations();

  OSecuritySystem getSecuritySystem();

  default Set<String> listLodadedDatabases() {
    throw new UnsupportedOperationException();
  }

  String getConnectionUrl();

  public default void startCommand(Optional<Long> timeout) {}

  public default void endCommand() {}
}
