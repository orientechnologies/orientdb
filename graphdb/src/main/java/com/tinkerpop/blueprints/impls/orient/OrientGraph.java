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

import org.apache.commons.configuration.Configuration;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.Features;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraph extends OrientTransactionalGraph {
  private boolean          featuresInitialized = false;

  protected final Features FEATURES            = new Features();

  /**
   * Creates a new Transactional Graph using an existent database instance. User and password are passed in case of re-open.
   *
   * @param iDatabase
   *          Underlying database object to attach
   */
  public OrientGraph(final ODatabaseDocumentTx iDatabase, final String iUserName, final String iUserPasswd) {
    super(iDatabase, true, iUserName, iUserPasswd);
  }

  /**
   * Creates a new Transactional Graph using an existent database instance and the auto-start setting to determine if auto start a
   * transaction.
   *
   * @param iDatabase
   *          Underlying database object to attach
   * @param iAutoStartTx
   *          True to auto start a transaction at the beginning and after each commit/rollback
   */
  public OrientGraph(final ODatabaseDocumentTx iDatabase, final boolean iAutoStartTx) {
    super(iDatabase, iAutoStartTx, null, null);
  }

  /**
   * Creates a new Transactional Graph from an URL using default user (admin) and password (admin).
   *
   * @param url
   *          OrientDB URL
   */
  public OrientGraph(final String url) {
    super(url, ADMIN, ADMIN);
  }

  /**
   * Creates a new Transactional Graph from an URL using default user (admin) and password (admin). It receives also the auto-start
   * setting to determine if auto start a transaction.
   *
   * @param url
   *          OrientDB URL
   * @param iAutoStartTx
   *          True to auto start a transaction at the beginning and after each commit/rollback
   */
  public OrientGraph(final String url, final boolean iAutoStartTx) {
    super(url, ADMIN, ADMIN, iAutoStartTx);
  }

  /**
   * Creates a new Transactional Graph from an URL using a username and a password.
   *
   * @param url
   *          OrientDB URL
   * @param username
   *          Database user name
   * @param password
   *          Database user password
   */
  public OrientGraph(final String url, final String username, final String password) {
    super(url, username, password);
  }

  /**
   *
   * Creates a new Transactional Graph from an URL using a username and a password. It receives also the auto-start setting to
   * determine if auto start a transaction.
   *
   * @param url
   *          OrientDB URL
   * @param username
   *          Database user name
   * @param password
   *          Database user password
   * @param iAutoStartTx
   *          True to auto start a transaction at the beginning and after each commit/rollback
   */
  public OrientGraph(final String url, final String username, final String password, final boolean iAutoStartTx) {
    super(url, username, password, iAutoStartTx);
  }

  /**
   * Creates a new Transactional Graph from a pool.
   *
   * @param pool
   *          Database pool where to acquire a database instance
   */
  public OrientGraph(final OPartitionedDatabasePool pool) {
    super(pool);
  }

  public OrientGraph(final OPartitionedDatabasePool pool, final Settings configuration) {
    super(pool, configuration);
  }

  /**
   * Builds a OrientGraph instance passing a configuration. Supported configuration settings are:
   * <table>
   * <tr>
   * <td><b>Name</b></td>
   * <td><b>Description</b></td>
   * <td><b>Default value</b></td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.url</td>
   * <td>Database URL</td>
   * <td>-</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.username</td>
   * <td>User name</td>
   * <td>admin</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.password</td>
   * <td>User password</td>
   * <td>admin</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.saveOriginalIds</td>
   * <td>Saves the original element IDs by using the property _id. This could be useful on import of graph to preserve original ids</td>
   * <td>false</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.keepInMemoryReferences</td>
   * <td>Avoid to keep records in memory but only RIDs</td>
   * <td>false</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.useCustomClassesForEdges</td>
   * <td>Use Edge's label as OrientDB class. If doesn't exist create it under the hood</td>
   * <td>true</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.useCustomClassesForVertex</td>
   * <td>Use Vertex's label as OrientDB class. If doesn't exist create it under the hood</td>
   * <td>true</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.useVertexFieldsForEdgeLabels</td>
   * <td>Store the edge relationships in vertex by using the Edge's class. This allow to use multiple fields and make faster
   * traversal by edge's label (class)</td>
   * <td>true</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.lightweightEdges</td>
   * <td>Uses lightweight edges. This avoid to create a physical document per edge. Documents are created only when they have
   * properties</td>
   * <td>true</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.autoStartTx</td>
   * <td>Auto start a transaction as soon the graph is changed by adding/remote vertices and edges and properties</td>
   * <td>true</td>
   * </tr>
   * </table>
   *
   * @param iConfiguration
   *          graph settings see the details above.
   */
  public OrientGraph(final Configuration iConfiguration) {
    super(iConfiguration);
  }

  /**
   * Creates a new Transactional Graph using an existent database instance.
   *
   * @param iDatabase
   *          Underlying database object to attach
   */
  public OrientGraph(final ODatabaseDocumentTx iDatabase) {
    super(iDatabase);
  }

  /**
   * Creates a new Transactional Graph using an existent database instance.
   *
   * @param iDatabase
   *          Underlying database object to attach
   */
  public OrientGraph(final ODatabaseDocumentTx iDatabase, final String iUser, final String iPassword, final Settings iConfiguration) {
    super(iDatabase, iUser, iPassword, iConfiguration);
  }

  /**
   * Returns the current Graph settings.
   *
   * @return Features object
   */
  public Features getFeatures() {
		makeActive();

    if (!featuresInitialized) {
      FEATURES.supportsDuplicateEdges = true;
      FEATURES.supportsSelfLoops = true;
      FEATURES.isPersistent = true;
      FEATURES.supportsVertexIteration = true;
      FEATURES.supportsVertexIndex = true;
      FEATURES.ignoresSuppliedIds = true;
      FEATURES.supportsTransactions = true;
      FEATURES.supportsVertexKeyIndex = true;
      FEATURES.supportsKeyIndices = true;
      FEATURES.isWrapper = false;
      FEATURES.supportsIndices = true;
      FEATURES.supportsVertexProperties = true;
      FEATURES.supportsEdgeProperties = true;

      // For more information on supported types, please see:
      // http://code.google.com/p/orient/wiki/Types
      FEATURES.supportsSerializableObjectProperty = true;
      FEATURES.supportsBooleanProperty = true;
      FEATURES.supportsDoubleProperty = true;
      FEATURES.supportsFloatProperty = true;
      FEATURES.supportsIntegerProperty = true;
      FEATURES.supportsPrimitiveArrayProperty = true;
      FEATURES.supportsUniformListProperty = true;
      FEATURES.supportsMixedListProperty = true;
      FEATURES.supportsLongProperty = true;
      FEATURES.supportsMapProperty = true;
      FEATURES.supportsStringProperty = true;
      FEATURES.supportsThreadedTransactions = false;
      FEATURES.supportsThreadIsolatedTransactions = false;

      // DYNAMIC FEATURES BASED ON CONFIGURATION
      FEATURES.supportsEdgeIndex = !isUseLightweightEdges();
      FEATURES.supportsEdgeKeyIndex = !isUseLightweightEdges();
      FEATURES.supportsEdgeIteration = !isUseLightweightEdges();
      FEATURES.supportsEdgeRetrieval = !isUseLightweightEdges();

      featuresInitialized = true;
    }

    return FEATURES;
  }
}
