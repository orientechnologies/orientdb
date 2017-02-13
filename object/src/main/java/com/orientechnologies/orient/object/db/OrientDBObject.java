package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;

import java.util.List;

/**
 * Created by tglman on 13/01/17.
 */
public class OrientDBObject implements AutoCloseable {

  private OrientDB orientDB;

  public OrientDBObject(String environment, OrientDBConfig config) {
    this(environment, null, null, config);
  }

  public OrientDBObject(String environment, String serverUser, String serverPassword, OrientDBConfig config) {
    this.orientDB = new OrientDB(environment, serverUser, serverPassword, config);
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
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) orientDB.open(name, user, password));
  }

  /**
   * Open a database specified by name using the username and password if needed, with specific configuration
   *
   * @param name     of the database to open
   * @param user     the username allowed to open the database
   * @param password related to the specified username
   * @param config   database specific configuration that override the orientDB global settings where needed.
   *
   * @return the opened database
   */
  public ODatabaseObject open(String name, String user, String password, OrientDBConfig config) {
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) orientDB.open(name, user, password, config));
  }

  /**
   * Create a new database
   *
   * @param name database name
   * @param type can be plocal or memory
   */
  public void create(String name, ODatabaseType type) {
    orientDB.create(name, type);
  }

  /**
   * Create a new database
   *
   * @param name   database name
   * @param type   can be plocal or memory
   * @param config database specific configuration that override the orientDB global settings where needed.
   */
  public void create(String name, ODatabaseType type, OrientDBConfig config) {
    orientDB.create(name, type, config);
  }

  /**
   * Check if a database exists
   *
   * @param name database name to check
   *
   * @return boolean true if exist false otherwise.
   */
  public boolean exists(String name) {
    return orientDB.exists(name);
  }

  /**
   * Drop a database
   *
   * @param name database name
   */
  public void drop(String name) {
    orientDB.drop(name);
  }

  /**
   * List of database exiting in the current environment
   *
   * @return a set of databases names.
   */
  public List<String> list() {
    return orientDB.list();
  }


  public void close() {
    orientDB.close();
  }

  protected OrientDB getOrientDB() {
    return orientDB;
  }
}
