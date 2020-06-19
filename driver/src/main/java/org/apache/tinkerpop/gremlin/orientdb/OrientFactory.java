package org.apache.tinkerpop.gremlin.orientdb;

import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_PASS;
import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_URL;
import static org.apache.tinkerpop.gremlin.orientdb.OrientGraph.CONFIG_USER;

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

/** OrientFactory is used to open a new OrientStandardGraph */
public class OrientFactory {

  /**
   * Opens a {@link OGraph} in-memory database
   *
   * @return OGraph database
   */
  public static OrientStandardGraph open() {
    return open("memory:orientdb-" + Math.random(), "admin", "admin");
  }

  /**
   * Opens a {@link OGraph}
   *
   * @param url URL for the specific database
   * @return OGraph database
   */
  public static OrientStandardGraph open(String url) {
    return open(url, "admin", "admin");
  }

  /**
   * Opens a {@link OGraph}
   *
   * @param url URL for the specific database
   * @param user username of a database user or a server user allowed to open the database
   * @param password related to the specified username
   * @return OGraph database
   */
  public static OrientStandardGraph open(String url, String user, String password) {
    BaseConfiguration configuration = new BaseConfiguration();
    configuration.setProperty(CONFIG_URL, url);
    configuration.setProperty(CONFIG_USER, user);
    configuration.setProperty(CONFIG_PASS, password);
    return open(configuration);
  }

  /**
   * Opens a {@link OGraph}
   *
   * @param config URL Configuration for the graph database
   * @return OGraph database
   */
  public static OrientStandardGraph open(final Configuration config) {
    boolean transactional = config.getBoolean(OrientGraph.CONFIG_TRANSACTIONAL, true);
    config.setProperty(OrientGraph.CONFIG_TRANSACTIONAL, transactional);
    OrientGraphFactory factory = new OrientGraphFactory(config);
    return new OrientStandardGraph(factory, config);
  }
}
