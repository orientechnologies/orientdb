package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;

/**
 * A factory to create instances of {@link OrientGraph}. OrientGraph is a Blueprints implementation of the graph database OrientDB
 * (http://www.orientechnologies.com).
 * 
 * By default creates new connection for each graph, but could be configured to use database pool.
 * 
 * Thread safe after set up.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraphFactory {
  protected final String                   url;
  protected final String                   user;
  protected final String                   password;

  protected volatile ODatabaseDocumentPool pool;
  protected volatile boolean               transactional = true;

  /**
   * Creates a factory that use default admin credentials.
   * 
   * @param iURL
   *          to the database
   */
  public OrientGraphFactory(final String iURL) {
    this(iURL, "admin", "admin");
  }

  /**
   * Creates a factory with given credentials.
   * 
   * @param iURL
   *          to the database
   * @param iUser
   *          name of the user
   * @param iPassword
   *          of the user
   */
  public OrientGraphFactory(final String iURL, final String iUser, final String iPassword) {
    url = iURL;
    user = iUser;
    password = iPassword;
  }

  /**
   * Closes all pooled databases and clear the pool.
   */
  public void close() {
    if (pool != null) {
      pool.close();
      pool = null;
    }
  }

  /**
   * Drops current database if such one exists.
   * 
   * @throws ODatabaseException
   *           if there is no current database.
   */
  public void drop() {
    getDatabase(false).drop();
  }

  /**
   * Depends of configuration work like {@link #getTx()} or {@link #getNoTx()}. By default the factory configured to return
   * transactional graph. Use {@link #setTransactional(boolean)} to change the behaviour
   * 
   * @return orient graph
   */
  public OrientBaseGraph get() {
    return transactional ? getTx() : getNoTx();
  }

  /**
   * Gets transactional graph with the database from pool if pool is configured. Otherwise creates a graph with new db instance.
   * 
   * @return transactional graph
   */
  public OrientGraph getTx() {
    if (pool == null) {
      return new OrientGraph(getDatabase());
    } else {
      return new OrientGraph(pool);
    }
  }

  /**
   * Gets non transactional graph with the database from pool if pool is configured. Otherwise creates a graph with new db instance.
   * 
   * @return non transactional graph
   */
  public OrientGraphNoTx getNoTx() {
    if (pool == null) {
      return new OrientGraphNoTx(getDatabase(), user, password);
    } else {
      return new OrientGraphNoTx(pool);
    }
  }

  /**
   * Gives new connection to database. If current factory configured to use pool (see {@link #setupPool(int, int)} method),
   * retrieves connection from pool. Otherwise creates new connection each time.
   * 
   * Automatically creates database if database with given URL does not exist
   * 
   * Shortcut for {@code getDatabase(true)}
   * 
   * @return database.
   */
  public ODatabaseDocumentTx getDatabase() {
    return getDatabase(true);
  }

  /**
   * Gives new connection to database. If current factory configured to use pool (see {@link #setupPool(int, int)} method),
   * retrieves connection from pool. Otherwise creates new connection each time.
   * 
   * @param iCreate
   *          if true automatically creates database if database with given URL does not exist
   * @return database
   */
  public ODatabaseDocumentTx getDatabase(final boolean iCreate) {
    if (pool != null)
      // USE THE POOL
      return pool.acquire();

    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    if (!db.getURL().startsWith("remote:") && !db.exists()) {
      if (iCreate)
        db.create();
      else
        throw new ODatabaseException("Database '" + url + "' not found");
    } else
      db.open(user, password);

    return db;
  }

  /**
   * Check if the database with path given to the factory exists.
   * 
   * @return true if database is exists
   */
  public boolean exists() {
    final ODatabaseDocumentTx db = getDatabase();
    try {
      return db.exists();
    } finally {
      db.close();
    }
  }

  /**
   * Setting up the factory to use database pool instead of creation a new instance of database connection each time.
   * 
   * @param iMin
   *          minimum size of pool
   * @param iMax
   *          maximum size of pool
   * @return this
   */
  public OrientGraphFactory setupPool(final int iMin, final int iMax) {
    // ASSURE THE DB IS CREATED THE FIRST TIME
    final ODatabaseDocumentTx db = getDatabase();
    db.close();

    if (pool != null) {
      pool.close();
    }

    pool = new ODatabaseDocumentPool(url, user, password);
    pool.setup(iMin, iMax);
    return this;
  }

  /**
   * @return true if current factory configured to create transactional graphs by default.
   * @see #get()
   */
  public boolean isTransactional() {
    return transactional;
  }

  /**
   * Configure current factory to create transactional of non transactional graphs by default.
   * 
   * @param transactional
   *          defines should the factory return transactional graph by default.
   * @return this
   * 
   * @see #get()
   */
  public OrientGraphFactory setTransactional(boolean transactional) {
    this.transactional = transactional;
    return this;
  }

  /**
   * Returns the number of available instances in the pool.
   */
  public int getAvailableInstancesInPool() {
    if (pool != null)
      return pool.getAvailableConnections(url, user);
    return 0;
  }

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }

}
