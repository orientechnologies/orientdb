/*
 *
 *  *  Copyright 2014 OrientDB LTD (info(at)orientdb.com)
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
 *  * For more information: http://www.orientdb.com
 *
 */
package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.OrientDB;

/**
 * Basic implementation of an Orient Graph factory that supports new OrientDB v 3.0 capabilities
 *
 * @author Luigi Dell'Aquila
 */
public class OrientGraphFactoryV2 {

  private final OrientDB orientDB;
  private String dbName;
  private ODatabasePool pool;

  /**
   * @param orientDB
   * @param dbName
   * @param username
   * @param password
   */
  public OrientGraphFactoryV2(OrientDB orientDB, String dbName, String username, String password) {
    this.orientDB = orientDB;
    this.dbName = dbName;
    pool = new ODatabasePool(orientDB, dbName, username, password);
  }

  /** Closes all pooled databases and clear the pool. */
  public void close() {
    if (pool != null) pool.close();

    pool = null;
  }

  /** Drops current database if such one exists. */
  public void drop() {
    orientDB.drop(dbName);
  }

  /**
   * Gets transactional graph with the database from pool if pool is configured. Otherwise creates a
   * graph with new db instance. The Graph instance inherits the factory's configuration.
   *
   * @return transactional graph
   */
  public OrientGraph getTx() {
    if (pool == null) {
      throw new IllegalStateException();
    }

    return new OrientGraph((ODatabaseDocumentInternal) pool.acquire());
  }

  /**
   * Gets non transactional graph with the database from pool if pool is configured. Otherwise
   * creates a graph with new db instance. The Graph instance inherits the factory's configuration.
   *
   * @return non transactional graph
   */
  public OrientGraphNoTx getNoTx() {
    if (pool == null) {
      throw new IllegalStateException();
    }

    return new OrientGraphNoTx((ODatabaseDocumentInternal) pool.acquire());
  }

  /**
   * Check if the database with path given to the factory exists.
   *
   * <p>this api can be used only in embedded mode, and has no need of authentication.
   *
   * @return true if database is exists
   */
  public boolean exists() {
    return orientDB.exists(dbName);
  }
}
