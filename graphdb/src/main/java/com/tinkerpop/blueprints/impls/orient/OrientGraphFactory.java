package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.intent.OIntent;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

public class OrientGraphFactory extends OrientConfigurableGraph {
  protected final String url;
  protected final String user;
  protected final String password;
  protected final Map<String, Object> properties = new HashMap<String, Object>();
  protected volatile OPartitionedDatabasePool pool;
  protected          OIntent                  intent;
  protected AtomicBoolean used = new AtomicBoolean(false);

  /**
   * Creates a factory that use default admin credentials and pool with maximum amount of connections equal to amount of CPU cores.
   *
   * @param iURL to the database
   */
  public OrientGraphFactory(final String iURL) {
    this(iURL, OrientBaseGraph.ADMIN, OrientBaseGraph.ADMIN);
  }

  /**
   * Creates a factory that use default admin credentials and pool with maximum amount of connections equal to amount of CPU cores
   * if needed.
   *
   * @param iURL       to the database
   * @param createPool flag which indicates whether pool should be created.
   */
  public OrientGraphFactory(final String iURL, boolean createPool) {
    this(iURL, OrientBaseGraph.ADMIN, OrientBaseGraph.ADMIN, createPool);
  }

  /**
   * Creates a factory with given credentials and pool with maximum amount of connections equal to amount of CPU cores.
   * <p>
   * If you wish to change pool settings call com.tinkerpop.blueprints.impls.orient.OrientGraphFactory#setupPool(int, int) method.
   *
   * @param iURL      to the database
   * @param iUser     name of the user
   * @param iPassword of the user
   */
  public OrientGraphFactory(final String iURL, final String iUser, final String iPassword) {
    this(iURL, iUser, iPassword, true);
  }

  /**
   * Creates a factory with given credentials and pool with maximum amount of connections equal to amount of CPU cores if that is
   * needed.
   * <p>
   * If you wish to change pool settings call com.tinkerpop.blueprints.impls.orient.OrientGraphFactory#setupPool(int, int) method.
   *
   * @param iURL       to the database
   * @param iUser      name of the user
   * @param iPassword  of the user
   * @param createPool flag which indicates whether pool should be created.
   */
  public OrientGraphFactory(final String iURL, final String iUser, final String iPassword, boolean createPool) {
    url = iURL;
    user = iUser;
    password = iPassword;
    if (createPool)
      pool = new OPartitionedDatabasePool(url, user, password, 64, Runtime.getRuntime().availableProcessors()).setAutoCreate(true);

  }

  /**
   * Creates a factory with given credentials also you may pass pool which you already use in "document part" of your application.
   * It is mandatory to use the same pool for document and graph databases.
   *
   * @param iURL      to the database
   * @param iUser     name of the user
   * @param iPassword password of the user
   * @param pool      Pool which is used in "document part" of your application.
   */
  public OrientGraphFactory(final String iURL, final String iUser, final String iPassword, OPartitionedDatabasePool pool) {
    url = iURL;
    user = iUser;
    password = iPassword;
    this.pool = pool;
  }

  /**
   * Closes all pooled databases and clear the pool.
   */
  public void close() {
    if (pool != null)
      pool.close();

    pool = null;
  }

  /**
   * Drops current database if such one exists.
   */
  public void drop() {
    getDatabase(false, true).drop();
  }

  /**
   * Gets transactional graph with the database from pool if pool is configured. Otherwise creates a graph with new db instance. The
   * Graph instance inherits the factory's configuration.
   *
   * @return transactional graph
   */
  public OrientGraph getTx() {
    final OrientGraph g;
    if (pool == null) {
      g = new OrientGraph(getDatabase(), user, password, settings);
    } else {
      // USE THE POOL
      g = new OrientGraph(pool, settings);
    }

    initGraph(g);
    return g;
  }

  /**
   * Gets non transactional graph with the database from pool if pool is configured. Otherwise creates a graph with new db instance.
   * The Graph instance inherits the factory's configuration.
   *
   * @return non transactional graph
   */
  public OrientGraphNoTx getNoTx() {
    final OrientGraphNoTx g;

    if (pool == null) {
      g = new OrientGraphNoTx(getDatabase(), user, password, settings);

    } else {
      // USE THE POOL
      g = new OrientGraphNoTx(pool, settings);
    }

    initGraph(g);
    return g;
  }

  /**
   * Gives new connection to database. If current factory configured to use pool (see {@link #setupPool(int, int)} method),
   * retrieves connection from pool. Otherwise creates new connection each time.
   * <p>
   * Automatically creates database if database with given URL does not exist
   * <p>
   * Shortcut for {@code getDatabase(true)}
   *
   * @return database.
   */
  public ODatabaseDocumentTx getDatabase() {
    return getDatabase(true, true);
  }

  /**
   * Gives new connection to database. If current factory configured to use pool (see {@link #setupPool(int, int)} method),
   * retrieves connection from pool. Otherwise creates new connection each time.
   *
   * @param iCreate if true automatically creates database if database with given URL does not exist
   * @param iOpen   if true automatically opens the database
   * @return database
   */
  public ODatabaseDocumentTx getDatabase(final boolean iCreate, final boolean iOpen) {
    if (pool != null)
      return pool.acquire();

    final ODatabaseDocument db = new ODatabaseDocumentTx(url);

    final String connMode = settings.getConnectionStrategy();
    db.setProperty(OStorageRemote.PARAM_CONNECTION_STRATEGY, connMode);

    properties.entrySet().forEach(e -> db.setProperty(e.getKey(), e.getValue()));

    if (!db.getURL().startsWith("remote:") && !db.exists()) {
      if (iCreate)
        db.create();
      else if (iOpen)
        throw new ODatabaseException("Database '" + url + "' not found");
    } else if (iOpen)
      db.open(user, password);

    return (ODatabaseDocumentTx) db;
  }

  /**
   * Check if the database with path given to the factory exists.
   * <p>
   * this api can be used only in embedded mode, and has no need of authentication.
   *
   * @return true if database is exists
   */
  public boolean exists() {
    final ODatabaseDocument db = getDatabase(false, false);
    try {
      return db.exists();
    } finally {
      db.close();
    }
  }

  /**
   * Setting up the factory to use database pool instead of creation a new instance of database connection each time.
   *
   * @param iMin minimum size of pool
   * @param iMax maximum size of pool
   * @return this
   */
  public OrientGraphFactory setupPool(final int iMin, final int iMax) {
    if (pool != null) {
      pool.close();
    }

    pool = new OPartitionedDatabasePool(url, user, password, 64, iMax).setAutoCreate(true);

    properties.entrySet().forEach(p -> pool.setProperty(p.getKey(), p.getValue()));
    return this;
  }

  /**
   * Returns the number of available instances in the pool.
   */
  public int getAvailableInstancesInPool() {
    if (pool != null)
      return pool.getAvailableConnections();
    return 0;
  }

  /**
   * Returns the total number of instances created in the pool.
   */
  public int getCreatedInstancesInPool() {
    if (pool != null)
      return pool.getCreatedInstances();

    return 0;
  }

  @Override
  public void declareIntent(final OIntent iIntent) {
    intent = iIntent;
  }

  protected void initGraph(final OrientBaseGraph g) {
    if (used.compareAndSet(false, true)) {
      // EXECUTE ONLY ONCE
      final ODatabaseDocument db = g.getRawGraph();
      boolean txActive = db.getTransaction().isActive();

      if (txActive)
        // COMMIT TX BEFORE ANY SCHEMA CHANGES
        db.commit();

      if (txActive) {
        // REOPEN IT AGAIN
        db.begin();
        db.getTransaction().setUsingLog(settings.isUseLog());
      }

    }

    if (intent != null)
      g.declareIntent(intent.copy());
  }

  /**
   * Sets a property value
   *
   * @param iName  Property name
   * @param iValue new value to set
   * @return The previous value if any, otherwise null
   */
  public Object setProperty(final String iName, final Object iValue) {

    if (pool != null)
      pool.setProperty(iName, iValue);

    if (iValue != null)
      return properties.put(iName.toLowerCase(), iValue);
    else
      return properties.remove(iName.toLowerCase());
  }

  /**
   * Gets the property value.
   *
   * @param iName Property name
   * @return The previous value if any, otherwise null
   */
  public Object getProperty(final String iName) {
    return properties.get(iName.toLowerCase());
  }

}
