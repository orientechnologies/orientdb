package com.orientechnologies.orient.object.db;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OrientDB;
import com.orientechnologies.orient.core.db.OrientDBConfig;
import com.orientechnologies.orient.core.db.object.ODatabaseObject;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.List;
import java.util.Map;

/**
 * OrientDB Object management environment, it allow to connect to an environment and manipulate
 * databases or open sessions.
 *
 * <p>Usage example:
 *
 * <p>Remote Example:
 *
 * <pre>
 * <code>
 * OrientDBObject orientDbObject = new OrientDBObject("remote:localhost","root","root");
 * if(orientDbObject.createIfNotExists("test",ODatabaseType.MEMORY)){
 *  ODatabaseDocument session = orientDbObject.open("test","admin","admin");
 *  session.createClass("MyClass");
 *  session.close();
 * }
 * ODatabaseObject session = orientDbObject.open("test","writer","writer");
 * //...
 * session.close();
 * orientDbObject.close();
 * </code>
 * </pre>
 *
 * <p>Embedded example:
 *
 * <pre>
 * <code>
 * OrientDBObject orientDbObject = new OrientDBObject("embedded:./databases/",null,null);
 * orientDbObject.create("test",ODatabaseType.PLOCAL);
 * ODatabaseObject session = orientDbObject.open("test","admin","admin");
 * //...
 * session.close();
 * orientDbObject.close();
 * </code>
 * </pre>
 *
 * <p>Database Manipulation Example:
 *
 * <p>
 *
 * <pre>
 * <code>
 * OrientDB orientDbObject = ...
 * if(!orientDbObject.exists("one")){
 *  orientDbObject.create("one",ODatabaseType.PLOCAL);
 * }
 * if(orientDbObject.exists("two")){
 *  orientDbObject.drop("two");
 * }
 * List&ltString&gt databases = orientDbObject.list();
 * assertEquals(databases.size(),1);
 * assertEquals(databases.get("0"),"one");
 * </code>
 * </pre>
 *
 * <p>
 *
 * <p>
 *
 * <p>
 *
 * <p>Created by tglman on 13/01/17.
 */
public class OrientDBObject implements AutoCloseable {

  private OrientDB orientDB;

  /**
   * Create a new OrientDb Object instance from a given {@link OrientDB}
   *
   * @param orientDB the given environment.
   */
  public OrientDBObject(OrientDB orientDB) {
    this.orientDB = orientDB;
  }

  /**
   * Create a new OrientDb Object instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OrientDBObject orientDbObject = new OrientDBObject("remote:localhost");
   * ODatabaseObject session = orientDbObject.open("test","admin","admin");
   * //...
   * session.close();
   * orientDbObject.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OrientDBObject orientDbObject = new OrientDBObject("embedded:./databases/");
   * ODatabaseObject session = orientDbObject.open("test","admin","admin");
   * //...
   * session.close();
   * orientDbObject.close();
   * </code>
   * </pre>
   *
   * @param environment the url for the specific environment.
   * @param config configuration for the specific environment for the list of option {@see
   *     OGlobalConfiguration}.
   */
  public OrientDBObject(String environment, OrientDBConfig config) {
    this(environment, null, null, config);
  }

  /**
   * Create a new OrientDB Object instance for a specific environment
   *
   * <p>possible kind of urls 'embedded','remote', for the case of remote and distributed can be
   * specified multiple nodes using comma.
   *
   * <p>Remote Example:
   *
   * <pre>
   * <code>
   * OrientDBObject orientDbObject = new OrientDBObject("remote:localhost","root","root");
   * orientDbObject.create("test",ODatabaseType.PLOCAL);
   * ODatabaseObject session = orientDbObject.open("test","admin","admin");
   * //...
   * session.close();
   * orientDbObject.close();
   * </code>
   * </pre>
   *
   * <p>Embedded Example:
   *
   * <pre>
   * <code>
   * OrientDBObject orientDbObject = new OrientDBObject("embedded:./databases/",null,null);
   * orientDbObject.create("test",ODatabaseType.MEMORY);
   * ODatabaseObject session = orientDbObject.open("test","admin","admin");
   * //...
   * session.close();
   * orientDbObject.close();
   * </code>
   * </pre>
   *
   * @param environment the url for the specific environment.
   * @param serverUser the server user allowed to manipulate databases.
   * @param serverPassword relative to the server user.
   * @param config configuration for the specific environment for the list of option {@see
   *     OGlobalConfiguration}.
   */
  public OrientDBObject(
      String environment, String serverUser, String serverPassword, OrientDBConfig config) {
    this.orientDB = new OrientDB(environment, serverUser, serverPassword, config);
  }

  /**
   * Open a database specified by name using the username and password if needed
   *
   * @param name of the database to open
   * @param user the username allowed to open the database
   * @param password related to the specified username
   * @return the opened database
   */
  public ODatabaseObject open(String name, String user, String password) {
    return new OObjectDatabaseTx((ODatabaseDocumentInternal) orientDB.open(name, user, password));
  }

  /**
   * Open a database specified by name using the username and password if needed, with specific
   * configuration
   *
   * @param name of the database to open
   * @param user the username allowed to open the database
   * @param password related to the specified username
   * @param config database specific configuration that override the orientDB global settings where
   *     needed.
   * @return the opened database
   */
  public ODatabaseObject open(String name, String user, String password, OrientDBConfig config) {
    return new OObjectDatabaseTx(
        (ODatabaseDocumentInternal) orientDB.open(name, user, password, config));
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
   * @param name database name
   * @param type can be plocal or memory
   * @param config database specific configuration that override the orientDB global settings where
   *     needed.
   */
  public void create(String name, ODatabaseType type, OrientDBConfig config) {
    orientDB.create(name, type, config);
  }

  public OResultSet execute(String script, Map<String, Object> params) {
    return orientDB.execute(script, params);
  }

  public OResultSet execute(String script, Object... params) {
    return orientDB.execute(script, params);
  }

  /**
   * Check if a database exists
   *
   * @param name database name to check
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
