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

import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import org.apache.commons.configuration.Configuration;

import com.orientechnologies.common.util.OPair;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Features;
import com.tinkerpop.blueprints.util.ExceptionFactory;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraph extends OrientTransactionalGraph {
  private boolean featuresInitialized = false;

  protected final Features FEATURES = new Features();

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
   * <td>Saves the original element IDs by using the property _id. This could be useful on import of graph to preserve original ids
   * </td>
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
  public OrientGraph(final ODatabaseDocumentTx iDatabase, final String iUser, final String iPassword,
      final Settings iConfiguration) {
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

  OrientEdge addEdgeInternal(final OrientVertex currentVertex, String label, final OrientVertex inVertex, final String iClassName,
      final String iClusterName, final Object... fields) {
    if (currentVertex.checkDeletedInTx())
      throw new ORecordNotFoundException("The vertex " + currentVertex.getIdentity() + " has been deleted");

    if (inVertex.checkDeletedInTx())
      throw new ORecordNotFoundException("The vertex " + inVertex.getIdentity() + " has been deleted");

    autoStartTransaction();

    // TEMPORARY STATIC LOCK TO AVOID MT PROBLEMS AGAINST OMVRBTreeRID
    final ODocument outDocument = currentVertex.getRecord();
    if (outDocument == null)
      throw new IllegalArgumentException("source vertex is invalid (rid=" + currentVertex.getIdentity() + ")");

    if (!ODocumentInternal.getImmutableSchemaClass(outDocument).isVertexType())
      throw new IllegalArgumentException("source record is not a vertex");

    ODocument inDocument = inVertex.getRecord();
    if (inDocument == null)
      throw new IllegalArgumentException("destination vertex is invalid (rid=" + inVertex.getIdentity() + ")");

    if (!ODocumentInternal.getImmutableSchemaClass(outDocument).isVertexType())
      throw new IllegalArgumentException("destination record is not a vertex");

    OIdentifiable to;
    OIdentifiable from;

    label = OrientBaseGraph.encodeClassName(label);
    if (label == null && iClassName != null)
      // RETRO-COMPATIBILITY WITH THE SYNTAX CLASS:<CLASS-NAME>
      label = OrientBaseGraph.encodeClassName(iClassName);

    if (isUseClassForEdgeLabel()) {
      final OrientEdgeType edgeType = getEdgeType(label);
      if (edgeType == null)
        // AUTO CREATE CLASS
        createEdgeType(label);
      else
        // OVERWRITE CLASS NAME BECAUSE ATTRIBUTES ARE CASE SENSITIVE
        label = edgeType.getName();
    }

    final String outFieldName = currentVertex.getConnectionFieldName(Direction.OUT, label,
        settings.isUseVertexFieldsForEdgeLabels());
    final String inFieldName = currentVertex.getConnectionFieldName(Direction.IN, label, settings.isUseVertexFieldsForEdgeLabels());

    // since the label for the edge can potentially get re-assigned
    // before being pushed into the OrientEdge, the
    // null check has to go here.
    if (label == null)
      throw ExceptionFactory.edgeLabelCanNotBeNull();

    OrientEdge edge = null;
    if (currentVertex.canCreateDynamicEdge(outDocument, inDocument, outFieldName, inFieldName, fields, label)) {
      // CREATE A LIGHTWEIGHT DYNAMIC EDGE
      from = currentVertex.rawElement;
      to = inDocument;
      if (edge == null) {
        if (settings.isKeepInMemoryReferences())
          edge = new OrientEdge(this, from.getIdentity(), to.getIdentity(), label);
        else
          edge = new OrientEdge(this, from, to, label);
      }
    } else {
      if (edge == null) {
        // CREATE THE EDGE DOCUMENT TO STORE FIELDS TOO
        edge = new OrientEdge(this, label, fields);

        if (settings.isKeepInMemoryReferences())
          edge.getRecord().fields(OrientBaseGraph.CONNECTION_OUT, currentVertex.rawElement.getIdentity(),
              OrientBaseGraph.CONNECTION_IN, inDocument.getIdentity());
        else
          edge.getRecord().fields(OrientBaseGraph.CONNECTION_OUT, currentVertex.rawElement, OrientBaseGraph.CONNECTION_IN,
              inDocument);
      }

      from = edge.getRecord();
      to = edge.getRecord();
    }

    if (settings.isKeepInMemoryReferences()) {
      // USES REFERENCES INSTEAD OF DOCUMENTS
      from = from.getIdentity();
      to = to.getIdentity();
    }

    edge.save(iClusterName);

    // OUT-VERTEX ---> IN-VERTEX/EDGE
    currentVertex.createLink(this, outDocument, to, outFieldName);

    // IN-VERTEX ---> OUT-VERTEX/EDGE
    currentVertex.createLink(this, inDocument, from, inFieldName);

    outDocument.save();
    inDocument.save();

    return edge;
  }

  /**
   * Removes the Edge from the Graph. Connected vertices aren't removed.
   */
  public void removeEdgeInternal(final OrientEdge edge) {
    // OUT VERTEX
    final OIdentifiable inVertexEdge = edge.vIn != null ? edge.vIn : edge.rawElement;

    final String edgeClassName = OrientBaseGraph.encodeClassName(edge.getLabel());

    final boolean useVertexFieldsForEdgeLabels = settings.isUseVertexFieldsForEdgeLabels();

    final OIdentifiable outVertex = edge.getOutVertex();
    ODocument outVertexRecord = null;
    boolean outVertexChanged = false;

    if (outVertex != null) {
      outVertexRecord = outVertex.getRecord();
      if (outVertexRecord != null) {
        final String outFieldName = OrientVertex.getConnectionFieldName(Direction.OUT, edgeClassName, useVertexFieldsForEdgeLabels);
        outVertexChanged = edge.dropEdgeFromVertex(inVertexEdge, outVertexRecord, outFieldName,
            outVertexRecord.field(outFieldName));
      }
    }

    // IN VERTEX
    final OIdentifiable outVertexEdge = edge.vOut != null ? edge.vOut : edge.rawElement;

    final OIdentifiable inVertex = edge.getInVertex();
    ODocument inVertexRecord = null;
    boolean inVertexChanged = false;

    if (inVertex != null) {
      inVertexRecord = inVertex.getRecord();
      if (inVertexRecord != null) {
        final String inFieldName = OrientVertex.getConnectionFieldName(Direction.IN, edgeClassName, useVertexFieldsForEdgeLabels);
        inVertexChanged = edge.dropEdgeFromVertex(outVertexEdge, inVertexRecord, inFieldName, inVertexRecord.field(inFieldName));
      }
    }

    if (outVertexChanged)
      outVertexRecord.save();
    if (inVertexChanged)
      inVertexRecord.save();

    if (edge.rawElement != null)
      // NON-LIGHTWEIGHT EDGE
      edge.removeRecord();
  }

  @Override
  void removeEdgesInternal(OrientVertex vertex, ODocument iVertex, OIdentifiable iVertexToRemove, boolean iAlsoInverse,
      boolean useVertexFieldsForEdgeLabels, boolean autoScaleEdgeType) {

    for (String fieldName : iVertex.fieldNames()) {
      final OPair<Direction, String> connection = vertex.getConnection(Direction.BOTH, fieldName);
      if (connection == null)
        // SKIP THIS FIELD
        continue;

      removeEdges(this, iVertex, fieldName, iVertexToRemove, iAlsoInverse, useVertexFieldsForEdgeLabels, autoScaleEdgeType, false);
    }
  }
}
