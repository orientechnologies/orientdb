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
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

/**
 * Created by tglman on 27/03/16.
 */
public interface OrientDBInternal extends AutoCloseable {

  /**
   * Create a new factory from a given url.
   * <p/>
   * possible kind of urls 'embedded','remote', for the case of remote and distributed can be specified multiple nodes
   * using comma.
   *
   * @param url           the url for the specific factory.
   * @param configuration configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   *
   * @return the new Orient Factory.
   */
  static OrientDBInternal fromUrl(String url, OrientDBConfig configuration) {
    String what = url.substring(0, url.indexOf(':'));
    if ("embedded".equals(what))
      return embedded(url.substring(url.indexOf(':') + 1), configuration);
    else if ("remote".equals(what))
      return remote(url.substring(url.indexOf(':') + 1).split(","), configuration);
    throw new ODatabaseException("not supported database type");
  }

  default OrientDB newOrientDB() {
    return new OrientDB(this);
  }

  /**
   * Create a new remote factory
   *
   * @param hosts         array of hosts
   * @param configuration configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   *
   * @return a new remote databases factory
   */
  static OrientDBInternal remote(String[] hosts, OrientDBConfig configuration) {
    OrientDBInternal factory;

    try {
      Class<?> kass = Class.forName("com.orientechnologies.orient.core.db.OrientDBRemote");
      Constructor<?> constructor = kass.getConstructor(String[].class, OrientDBConfig.class, Orient.class);
      factory = (OrientDBInternal) constructor.newInstance(hosts, configuration, Orient.instance());
    } catch (ClassNotFoundException | NoSuchMethodException | IllegalAccessException | InstantiationException e) {
      throw new ODatabaseException("OrientDB client API missing");
    } catch (InvocationTargetException e) {
      throw OException.wrapException(new ODatabaseException("Error creating OrientDB remote factory"), e.getTargetException());
    }
    return factory;
  }

  /**
   * Create a new Embedded factory
   *
   * @param directoryPath base path where the database are hosted
   * @param config        configuration for the specific factory for the list of option {@see OGlobalConfiguration}
   *
   * @return a new embedded databases factory
   */
  static OrientDBInternal embedded(String directoryPath, OrientDBConfig config) {
    return new OrientDBEmbedded(directoryPath, config, Orient.instance());
  }

  /**
   * Open a database specified by name using the username and password if needed
   *
   * @param name     of the database to open
   * @param user     the username allowed to open the database
   * @param password related to the specified username
   *
   * @return the opened database
   */
  ODatabaseDocument open(String name, String user, String password);

  /**
   * Open a database specified by name using the username and password if needed, with specific configuration
   *
   * @param name     of the database to open
   * @param user     the username allowed to open the database
   * @param password related to the specified username
   * @param config   database specific configuration that override the factory global settings where needed.
   *
   * @return the opened database
   */
  ODatabaseDocument open(String name, String user, String password, OrientDBConfig config);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user     the username of a user allowed to create a database, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   * @param type     can be plocal or memory
   */
  void create(String name, String user, String password, ODatabaseType type);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user     the username of a user allowed to create a database, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   * @param config   database specific configuration that override the factory global settings where needed.
   * @param type     can be plocal or memory
   */
  void create(String name, String user, String password, ODatabaseType type, OrientDBConfig config);

  /**
   * Check if a database exists
   *
   * @param name     database name to check
   * @param user     the username of a user allowed to check the database existence, in case of remote is a server user for embedded
   *                 it can be left empty.
   * @param password the password relative to the user
   *
   * @return boolean true if exist false otherwise.
   */
  boolean exists(String name, String user, String password);

  /**
   * Drop a database
   *
   * @param name     database name
   * @param user     the username of a user allowed to drop a database, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   */
  void drop(String name, String user, String password);

  /**
   * List of database exiting in the current environment
   *
   * @param user     the username of a user allowed to list databases, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   *
   * @return a set of databases names.
   */
  Set<String> listDatabases(String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name     database name
   * @param user     the username allowed to open the database
   * @param password the password relative to the user
   *
   * @return a new pool of databases.
   */
  ODatabasePoolInternal openPool(String name, String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name     database name
   * @param user     the username allowed to open the database
   * @param password the password relative to the user
   * @param config   database specific configuration that override the factory global settings where needed.
   *
   * @return a new pool of databases.
   */
  ODatabasePoolInternal openPool(String name, String user, String password, OrientDBConfig config);

  /**
   * Close the factory with all related databases and pools.
   */
  void close();

}
