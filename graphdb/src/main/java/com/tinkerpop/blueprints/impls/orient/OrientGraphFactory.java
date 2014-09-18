package com.tinkerpop.blueprints.impls.orient;

import java.util.concurrent.atomic.AtomicBoolean;

import com.orientechnologies.common.concur.resource.OResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.db.ODatabaseComplex;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.ODatabaseLifecycleListener;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecordInternal;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.intent.OIntent;

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
public class OrientGraphFactory extends OrientConfigurableGraph implements ODatabaseLifecycleListener {
  protected final String                                    url;
  protected final String                                    user;
  protected final String                                    password;
  protected volatile OResourcePool<String, OrientBaseGraph> pool;
  protected volatile boolean                                transactional   = true;
  protected boolean                                         leaveGraphsOpen = true;
  protected OIntent                                         intent;
  protected AtomicBoolean                                   used            = new AtomicBoolean(false);

  /**
   * Creates a factory that use default admin credentials.
   *
   * @param iURL
   *          to the database
   */
  public OrientGraphFactory(final String iURL) {
    this(iURL, OrientBaseGraph.ADMIN, OrientBaseGraph.ADMIN);
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
    Orient.instance().addDbLifecycleListener(this);
  }

  public boolean isLeaveGraphsOpen() {
    return leaveGraphsOpen;
  }

  public void setLeaveGraphsOpen(boolean leaveGraphsOpen) {
    this.leaveGraphsOpen = leaveGraphsOpen;
  }

  /**
   * Closes all pooled databases and clear the pool.
   */
  public void close() {
    closePool();
    pool = null;
    Orient.instance().removeDbLifecycleListener(this);
  }

  @Override
  public PRIORITY getPriority() {
    return PRIORITY.FIRST;
  }

  /**
   * Drops current database if such one exists.
   *
   * @throws ODatabaseException
   *           if there is no current database.
   */
  public void drop() {
    getDatabase(false, true).drop();
  }

  /**
   * Depends of configuration work like {@link #getTx()} or {@link #getNoTx()}. By default the factory configured to return
   * transactional graph. Use {@link #setTransactional(boolean)} to change the behaviour. The Graph instance inherits the factory's
   * configuration.
   *
   * @return orient graph
   */
  public OrientBaseGraph get() {
    return transactional ? getTx() : getNoTx();
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
      g = (OrientGraph) new OrientGraph(getDatabase()).configure(settings);
      initGraph(g);
    } else {
      if (!transactional)
        throw new IllegalStateException(
            "Cannot create a transactional graph instance after the pool has been set as non-transactional");

      // USE THE POOL
      return (OrientGraph) pool.getResource(url, 0, user, password);
    }

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
      g = (OrientGraphNoTx) new OrientGraphNoTx(getDatabase(), user, password).configure(settings);
      initGraph(g);
    } else {
      if (transactional)
        throw new IllegalStateException(
            "Cannot create a non-transactional graph instance after the pool has been set as transactional");

      // USE THE POOL
      return (OrientGraphNoTx) pool.getResource(url, 0, user, password);
    }

    return g;
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
    return getDatabase(true, true);
  }

  /**
   * Gives new connection to database. If current factory configured to use pool (see {@link #setupPool(int, int)} method),
   * retrieves connection from pool. Otherwise creates new connection each time.
   * 
   * @param iCreate
   *          if true automatically creates database if database with given URL does not exist
   * @param iOpen
   *          if true automatically opens the database
   * @return database
   */
  public ODatabaseDocumentTx getDatabase(final boolean iCreate, final boolean iOpen) {
    final ODatabaseDocumentTx db = new ODatabaseDocumentTx(url);
    if (!db.getURL().startsWith("remote:") && !db.exists()) {
      if (iCreate)
        db.create();
      else if (iOpen)
        throw new ODatabaseException("Database '" + url + "' not found");
    } else if (iOpen)
      db.open(user, password);

    return db;
  }

  /**
   * Check if the database with path given to the factory exists.
   * 
   * @return true if database is exists
   */
  public boolean exists() {
    final ODatabaseDocumentTx db = getDatabase(false, false);
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
    // // ASSURE THE DB IS CREATED THE FIRST TIME
    // final ODatabaseDocumentTx db = getDatabase();
    // db.close();
    //
    // CLOSE ANY PREVIOUS POOL
    closePool();

    pool = new OResourcePool<String, OrientBaseGraph>(iMax, new OResourcePoolListener<String, OrientBaseGraph>() {
      @Override
      public OrientBaseGraph createNewResource(final String iKey, final Object... iAdditionalArgs) {
        final OrientBaseGraph g;
        if (transactional)
          g = new OrientGraph(getDatabase(), user, password) {
            @Override
            public void shutdown() {
              if (pool != null)
                pool.returnResource(this);
              else
                super.shutdown();
            }
          }.configure(settings);
        else
          g = new OrientGraphNoTx(getDatabase(), user, password) {
            @Override
            public void shutdown() {
              if (pool != null) {
                pool.returnResource(this);
                ODatabaseRecordThreadLocal.INSTANCE.remove();
              } else
                super.shutdown();
            }
          }.configure(settings);

        initGraph(g);
        return g;
      }

      @Override
      public boolean reuseResource(final String iKey, final Object[] iAdditionalArgs, final OrientBaseGraph iValue) {
        // LEAVE THE DATABASE OPEN
        ODatabaseRecordThreadLocal.INSTANCE.set(iValue.getRawGraph());
        return true;
      }
    });

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
   * @param iTransactional
   *          defines should the factory return transactional graph by default.
   * @return this
   * 
   * @see #get()
   */
  public OrientGraphFactory setTransactional(final boolean iTransactional) {
    if (pool != null && transactional != iTransactional)
      throw new IllegalArgumentException("Cannot change transactional state after creating the pool");
    this.transactional = iTransactional;
    return this;
  }

  /**
   * Returns the number of available instances in the pool.
   */
  public int getAvailableInstancesInPool() {
    if (pool != null)
      return pool.getAvailableResources();
    return 0;
  }

  @Override
  public void declareIntent(final OIntent iIntent) {
    intent = iIntent;
  }

  @Override
  public void onCreate(final ODatabaseInternal iDatabase) {
    if (iDatabase instanceof ODatabaseRecordInternal) {
      final ODatabaseComplex<?> db = ((ODatabaseRecordInternal) iDatabase).getDatabaseOwner();

      if (db instanceof ODatabaseDocumentTx)
        OrientBaseGraph.checkForGraphSchema((ODatabaseDocumentTx) db);
    }
  }

  @Override
  public void onOpen(final ODatabaseInternal iDatabase) {
    if (iDatabase instanceof ODatabaseRecordInternal) {
      final ODatabaseComplex<?> db = ((ODatabaseRecordInternal) iDatabase).getDatabaseOwner();
      if (db instanceof ODatabaseDocumentTx)
        OrientBaseGraph.checkForGraphSchema((ODatabaseDocumentTx) db);
    }
  }

  @Override
  public void onClose(final ODatabaseInternal iDatabase) {

  }

  protected void closePool() {
    if (pool != null) {
      final OResourcePool<String, OrientBaseGraph> closingPool = pool;
      pool = null;

      int usedResources = 0;
      // CLOSE ALL THE INSTANCES
      for (OrientExtendedGraph g : closingPool.getResources()) {
        usedResources++;
        g.shutdown();
      }

      OLogManager.instance().debug(this, "Maximum used resources: %d", usedResources);

      closingPool.close();
    }
  }

  protected void initGraph(final OrientBaseGraph g) {
    if (used.compareAndSet(false, true)) {
      // EXECUTE ONLY ONCE
      final ODatabaseDocumentTx db = g.getRawGraph();
      boolean txActive = db.getTransaction().isActive();

      if (txActive)
        // COMMIT TX BEFORE ANY SCHEMA CHANGES
        db.commit();

      g.checkForGraphSchema(db);

      if (txActive)
        // REOPEN IT AGAIN
        db.begin();

    }

    if (intent != null)
      g.declareIntent(intent.copy());
  }

  @Override
  protected void finalize() throws Throwable {
    close();
    super.finalize();
  }
}
