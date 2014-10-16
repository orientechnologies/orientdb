/*
 *
 *  *  Copyright 2014 Orient Technologies LTD (info(at)orientechnologies.com)
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *  * For more information: http://www.orientechnologies.com
 *
 */

package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.intent.OIntent;

import java.util.concurrent.atomic.AtomicBoolean;

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
public class OrientGraphFactory extends OrientConfigurableGraph {
  protected final String                   url;
  protected final String                   user;
  protected final String                   password;
  protected volatile ODatabaseDocumentPool pool;
  protected OIntent                        intent;
  protected AtomicBoolean                  used = new AtomicBoolean(false);

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
   *
   * @throws ODatabaseException
   *           if there is no current database.
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
      g = new OrientGraph(pool);
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
      g = new OrientGraphNoTx(pool);
    }

    initGraph(g);
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
    // CLOSE ANY PREVIOUS POOL
    if (pool != null)
      pool.close();

    pool = new ODatabaseDocumentPool(url, user, password);
    pool.setup(iMin, iMax);

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

  /**
   * Returns the total number of instances created in the pool.
   */
  public int getCreatedInstancesInPool() {
    if (pool != null)
      return pool.getCreatedInstances(url, user);

    return 0;
  }

  @Override
  public void declareIntent(final OIntent iIntent) {
    intent = iIntent;
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
