package com.orientechnologies.orient.core.db;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Set;

/**
 * Created by tglman on 27/03/16.
 */
public interface OrientDBFactory extends AutoCloseable {

  enum DatabaseType {
    PLOCAL, MEMORY
  }

  /**
   * Create a new factory from a given url.
   * <p/>
   * possible kind of urls 'local','remote','distributed', for the case of remote and distributed can be specified multiple nodes using comma.
   *
   * @param url           the url for the specific factory.
   * @param configuration configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   * @return the new Orient Factory.
   */
  static OrientDBFactory fromUrl(String url, OrientDBSettings configuration) {
    String what = url.substring(0, url.indexOf(':'));
    if ("embedded".equals(what))
      return embedded(url.substring(url.indexOf(':') + 1), configuration);
    else if ("remote".equals(what))
      return remote(url.substring(url.indexOf(':') + 1).split(","), configuration);
    throw new ODatabaseException("not supported database type");
  }

  /**
   * Create a new remote factory
   *
   * @param hosts         array of hosts
   * @param configuration configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   * @return a new remote databases factory
   */
  static OrientDBFactory remote(String[] hosts, OrientDBSettings configuration) {
    OrientDBFactory factory;

    try {
      Class<?> kass = Class.forName("com.orientechnologies.orient.core.db.ORemoteDBFactory");
      Constructor<?> constructor = kass.getConstructor(String[].class, OrientDBSettings.class);
      factory = (OrientDBFactory) constructor.newInstance(hosts, configuration);
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
   * @param configuration configuration for the specific factory for the list of option {@see OGlobalConfiguration}
   * @return a new embedded databases factory
   */
  static OEmbeddedDBFactory embedded(String directoryPath, OrientDBSettings configuration) {
    return new OEmbeddedDBFactory(directoryPath, configuration);
  }

  /**
   * Open a database specified by name using the username or password if needed
   *
   * @param name     of the database to open
   * @param user     the username allowed to open the database
   * @param password related to the specified username
   * @return the opened database
   */
  ODatabaseDocument open(String name, String user, String password);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user     the username of a user allowed to create a database, in case of remote is a server user for embedded it can be left empty
   * @param password the password relative to the user
   * @param type     can be plocal or memory
   */
  void create(String name, String user, String password, DatabaseType type);

  /**
   * Check if a database exists
   *
   * @param name     database name to check
   * @param user     the username of a user allowed to check the database existence, in case of remote is a server user for embedded it can be left empty.
   * @param password the password relative to the user
   * @return boolean  true if exist false otherwise.
   */
  boolean exists(String name, String user, String password);

  /**
   * Drop a database
   *
   * @param name     database name
   * @param user     the username of a user allowed to drop a database, in case of remote is a server user for embedded it can be left empty
   * @param password the password relative to the user
   */
  void drop(String name, String user, String password);

  /**
   * List of database exiting in the current environment
   *
   * @param user     the username of a user allowed to list databases, in case of remote is a server user for embedded it can be left empty
   * @param password the password relative to the user
   * @return a set of databases names.
   */
  Set<String> listDatabases(String user, String password);

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name database name
   * @param user  the username allowed to open the database
   * @param password the password relative to the user
   * @return a new pool of databases.
   */
  OPool<ODatabaseDocument> openPool(String name, String user, String password);

  /**
   * Close the factory with all related databases and pools.
   */
  void close();

}
