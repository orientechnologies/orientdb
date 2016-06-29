package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.exception.ODatabaseException;

import java.util.Map;

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
   * @param configuration a map contain the configuration for the specific factory for the list of option {@see OGlobalConfiguration}.
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
   * @param configuration
   * @return
   */
  static OrientDBFactory remote(String[] hosts, OrientDBSettings configuration) {
    return new ORemoteDBFactory(hosts, configuration);
  }

  /**
   * Create a new Embedded factory
   *
   * @param directoryPath base path where the database are hosted.
   * @param configuration
   * @return
   */
  static OEmbeddedDBFactory embedded(String directoryPath, OrientDBSettings configuration) {
    return new OEmbeddedDBFactory(directoryPath, configuration);
  }

  /**
   * Open a database specified by name using the username or password if needed
   *
   * @param name     of the database to open.
   * @param user
   * @param password
   * @return the opened database.
   */
  ODatabaseDocument open(String name, String user, String password);

  /**
   * Create a new database
   *
   * @param name     database name
   * @param user
   * @param password
   * @param type     can be plocal or memory
   */
  void create(String name, String user, String password, DatabaseType type);

  boolean exist(String name, String user, String password);

  void drop(String name, String user, String password);

  Map<String, String> listDatabases(String user, String password);

  OPool<ODatabaseDocument> openPool(String name, String user, String password, Map<String, Object> poolSettings);

  void close();

}
