package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.util.Map;

/**
 * Created by tglman on 27/03/16.
 */
public abstract class OrientFactory implements AutoCloseable {

  public enum DatabaseType {
    PLOCAL, MEMORY
  }

  /**
   * Create a new factory from a given url.
   * <p/>
   * possible kind of urls 'local','remote','distributed', for the case of remote and distributed can be specified multiple nodes using comma.
   *
   * @param url           the url for the specific factory.
   * @param configuration a map contain the configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
   * @return the new Orient Factory.
   */
  public static OrientFactory fromUrl(String url, Map<String, String> configuration) {
    String what = url.substring(0, url.indexOf(':'));
    if ("local".equals(what))
      return embedded(url.substring(url.indexOf(':') + 1), configuration);
    else if ("remote".equals(what))
      return remote(url.substring(url.indexOf(':') + 1).split(","), configuration);
    else if ("distributed".equals(what))
      return join(url.substring(url.indexOf(':') + 1).split(","), configuration);
    throw new ODatabaseException("not supported database type");
  }

  /**
   * Create an embedded distributed factory.
   *
   * @param nodes
   * @param configuration
   * @return
   */
  public static ODistributedFactory join(String[] nodes, Map<String, String> configuration) {
    return new ODistributedFactory(nodes);
  }

  /**
   * Create a new remote factory
   *
   * @param hosts         array of hosts
   * @param configuration
   * @return
   */
  public static OrientFactory remote(String[] hosts, Map<String, String> configuration) {
    return new ORemoteFactory(hosts);
  }

  /**
   * Create a new Embedded factory
   *
   * @param directoryPath base path where the database are hosted.
   * @param configuration
   * @return
   */
  public static OEmbeddedFactory embedded(String directoryPath, Map<String, String> configuration) {
    return new OEmbeddedFactory(directoryPath);
  }

  /**
   * Open a database specified by name using the username or password if needed
   *
   * @param name     of the database to open.
   * @param user
   * @param password
   * @return the opened database.
   */
  public abstract ODatabaseDocument open(String name, String user, String password);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user
   * @param password
   * @param type     can be plocal or memory
   */
  public abstract void create(String name, String user, String password, DatabaseType type);

  public abstract boolean exist(String name, String user, String password);

  public abstract void drop(String name, String user, String password);

  public abstract Map<String, String> listDatabases(String user, String password);

  public abstract Pool<ODatabaseDocument> openPool(String name, String user, String password,Map<String,Object> poolSettings);

  public abstract void close();

}
