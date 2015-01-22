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
public class OrientGraphNoTx extends OrientBaseGraph {
  private final Features FEATURES = new Features();

  /**
   * Constructs a new object using an existent database instance.
   * 
   * @param iDatabase
   *          Underlying database object to attach
   */
  public OrientGraphNoTx(final ODatabaseDocumentTx iDatabase) {
    super(iDatabase, null, null, null);
    config();
  }

  public OrientGraphNoTx(OPartitionedDatabasePool pool) {
    super(pool);
    config();
  }

  public OrientGraphNoTx(OPartitionedDatabasePool pool, final Settings configuration) {
    super(pool, configuration);
    config();
  }

  public OrientGraphNoTx(final String url) {
    super(url, ADMIN, ADMIN);
    config();
  }

  public OrientGraphNoTx(final String url, final String username, final String password) {
    super(url, username, password);
    config();
  }

  public OrientGraphNoTx(final Configuration configuration) {
    super(configuration);
    config();
  }

  public OrientGraphNoTx(final ODatabaseDocumentTx iDatabase, final String user, final String password) {
    super(iDatabase, user, password, null);
    config();
  }

  public OrientGraphNoTx(final ODatabaseDocumentTx iDatabase, final String user, final String password,
      final Settings iConfiguration) {
    super(iDatabase, user, password, iConfiguration);
    config();
  }

  public Features getFeatures() {
    makeActive();

    // DYNAMIC FEATURES BASED ON CONFIGURATION
    FEATURES.supportsEdgeIndex = !isUseLightweightEdges();
    FEATURES.supportsEdgeKeyIndex = !isUseLightweightEdges();
    FEATURES.supportsEdgeIteration = !isUseLightweightEdges();
    FEATURES.supportsEdgeRetrieval = !isUseLightweightEdges();
    return FEATURES;
  }

  protected void config() {
    FEATURES.supportsDuplicateEdges = true;
    FEATURES.supportsSelfLoops = true;
    FEATURES.isPersistent = true;
    FEATURES.supportsVertexIteration = true;
    FEATURES.supportsVertexIndex = true;
    FEATURES.ignoresSuppliedIds = true;
    FEATURES.supportsTransactions = false;
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
  }
}
