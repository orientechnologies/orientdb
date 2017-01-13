package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.OrientDBFactory;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;

import java.util.Set;

/**
 * Created by tglman on 13/01/17.
 */
public class OrientDBObject implements AutoCloseable {

  private OrientDBFactory factory;

  private OrientDBObject(OrientDBFactory factory) {
    this.factory = factory;
  }

  public static OrientDBObject fromUrl(String url, OrientDBConfig config) {
    return new OrientDBObject(OrientDBFactory.fromUrl(url, config));
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
  public ODatabaseObject open(String name, String user, String password) {
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) factory.open(name, user, password));
  }

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
  public ODatabaseObject open(String name, String user, String password, OrientDBConfig config) {
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) factory.open(name, user, password, config));
  }

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user     the username of a user allowed to create a database, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   * @param type     can be plocal or memory
   */
  public void create(String name, String user, String password, OrientDBFactory.DatabaseType type) {
    factory.create(name, user, password, type);
  }

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
  public void create(String name, String user, String password, OrientDBFactory.DatabaseType type, OrientDBConfig config) {
    factory.create(name, user, password, type, config);
  }

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
  public boolean exists(String name, String user, String password) {
    return factory.exists(name, user, password);
  }

  /**
   * Drop a database
   *
   * @param name     database name
   * @param user     the username of a user allowed to drop a database, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   */
  public void drop(String name, String user, String password) {
    factory.drop(name, user, password);
  }

  /**
   * List of database exiting in the current environment
   *
   * @param user     the username of a user allowed to list databases, in case of remote is a server user for embedded it can be
   *                 left empty
   * @param password the password relative to the user
   *
   * @return a set of databases names.
   */
  public Set<String> listDatabases(String user, String password) {
    return factory.listDatabases(user, password);
  }

  /**
   * Open a pool of databases, similar to open but with multiple instances.
   *
   * @param name     database name
   * @param user     the username allowed to open the database
   * @param password the password relative to the user
   *
   * @return a new pool of databases.
   */
  public ODatabaseObjectPool openPool(String name, String user, String password) {
    return new ODatabaseObjectPool(factory.openPool(name, user, password));
  }

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
  public ODatabaseObjectPool openPool(String name, String user, String password, OrientDBConfig config) {
    return new ODatabaseObjectPool(factory.openPool(name, user, password, config));
  }

  public void close() {
    factory.close();
  }

}
