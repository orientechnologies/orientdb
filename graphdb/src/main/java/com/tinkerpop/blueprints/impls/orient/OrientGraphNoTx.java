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

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
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

  OrientEdge addEdge(final OrientVertex currentVertex, String label, final OrientVertex inVertex, final String iClassName,
      final String iClusterName, final Object... fields) {
    return OrientGraphNoTx.addEdge(this, currentVertex, label, inVertex, iClassName, iClusterName, fields);
  }

  static OrientEdge addEdge(final OrientBaseGraph graph, final OrientVertex currentVertex, String label,
      final OrientVertex inVertex, final String iClassName, final String iClusterName, final Object... fields) {

    OrientEdge edge = null;
    ODocument outDocument = null;
    ODocument inDocument = null;
    boolean outDocumentModified = false;

    for (int retry = 0; retry < graph.getMaxRetries(); ++retry) {
      try {
        // TEMPORARY STATIC LOCK TO AVOID MT PROBLEMS AGAINST OMVRBTreeRID
        if (outDocument == null) {
          outDocument = currentVertex.getRecord();
          if (outDocument == null)
            throw new IllegalArgumentException("source vertex is invalid (rid=" + currentVertex.getIdentity() + ")");
        }

        if (!ODocumentInternal.getImmutableSchemaClass(outDocument).isVertexType())
          throw new IllegalArgumentException("source record is not a vertex");

        if (inDocument == null) {
          inDocument = inVertex.getRecord();
          if (inDocument == null)
            throw new IllegalArgumentException("destination vertex is invalid (rid=" + inVertex.getIdentity() + ")");
        }

        if (!ODocumentInternal.getImmutableSchemaClass(outDocument).isVertexType())
          throw new IllegalArgumentException("destination record is not a vertex");

        OIdentifiable to;
        OIdentifiable from;

        label = OrientBaseGraph.encodeClassName(label);
        if (label == null && iClassName != null)
          // RETRO-COMPATIBILITY WITH THE SYNTAX CLASS:<CLASS-NAME>
          label = OrientBaseGraph.encodeClassName(iClassName);

        if (graph != null && graph.isUseClassForEdgeLabel()) {
          final OrientEdgeType edgeType = graph.getEdgeType(label);
          if (edgeType == null)
            // AUTO CREATE CLASS
            graph.createEdgeType(label);
          else
            // OVERWRITE CLASS NAME BECAUSE ATTRIBUTES ARE CASE SENSITIVE
            label = edgeType.getName();
        }

        final String outFieldName = currentVertex.getConnectionFieldName(Direction.OUT, label,
            graph.settings.isUseVertexFieldsForEdgeLabels());
        final String inFieldName = currentVertex.getConnectionFieldName(Direction.IN, label,
            graph.settings.isUseVertexFieldsForEdgeLabels());

        // since the label for the edge can potentially get re-assigned
        // before being pushed into the OrientEdge, the
        // null check has to go here.
        if (label == null)
          throw ExceptionFactory.edgeLabelCanNotBeNull();

        final ODocument edgeRecord;
        final boolean lightWeightEdge;
        if (currentVertex.canCreateDynamicEdge(outDocument, inDocument, outFieldName, inFieldName, fields, label)) {
          // CREATE A LIGHTWEIGHT DYNAMIC EDGE
          lightWeightEdge = true;
          from = currentVertex.rawElement;
          to = inDocument;
          if (edge == null) {
            if (graph.settings.isKeepInMemoryReferences())
              edge = new OrientEdge(graph, from.getIdentity(), to.getIdentity(), label);
            else
              edge = new OrientEdge(graph, from, to, label);
          }
          edgeRecord = null;
        } else {
          lightWeightEdge = false;
          if (edge == null) {
            // CREATE THE EDGE DOCUMENT TO STORE FIELDS TOO
            edge = new OrientEdge(graph, label, fields);
            edgeRecord = edge.getRecord();

            if (graph.settings.isKeepInMemoryReferences())
              edgeRecord.fields(OrientBaseGraph.CONNECTION_OUT, currentVertex.rawElement.getIdentity(),
                  OrientBaseGraph.CONNECTION_IN, inDocument.getIdentity());
            else
              edgeRecord.fields(OrientBaseGraph.CONNECTION_OUT, currentVertex.rawElement, OrientBaseGraph.CONNECTION_IN,
                  inDocument);
          } else
            edgeRecord = edge.getRecord();

          from = edgeRecord;
          to = edgeRecord;
        }

        if (graph.settings.isKeepInMemoryReferences()) {
          // USES REFERENCES INSTEAD OF DOCUMENTS
          from = from.getIdentity();
          to = to.getIdentity();
        }

        if (graph != null && edgeRecord != null)
          edgeRecord.save(iClusterName);

        if (!outDocumentModified) {
          // OUT-VERTEX ---> IN-VERTEX/EDGE
          currentVertex.createLink(graph, outDocument, to, outFieldName);

          if (graph != null) {
            outDocument.save();
            outDocumentModified = true;
          }
        }

        // IN-VERTEX ---> OUT-VERTEX/EDGE
        currentVertex.createLink(graph, inDocument, from, inFieldName);

        if (graph != null)
          inDocument.save();

        // OK
        break;

      } catch (ONeedRetryException e) {
        // RETRY
        if (!outDocumentModified)
          outDocument.reload();
        else if (inDocument != null)
          inDocument.reload();
      } catch (RuntimeException e) {
        // REVERT CHANGES. EDGE.REMOVE() TAKES CARE TO UPDATE ALSO BOTH VERTICES IN CASE
        try {
          edge.remove();
        } catch (Exception ex) {
        }
        throw e;
      } catch (Throwable e) {
        // REVERT CHANGES. EDGE.REMOVE() TAKES CARE TO UPDATE ALSO BOTH VERTICES IN CASE
        try {
          edge.remove();
        } catch (Exception ex) {
        }
        throw OException.wrapException(new OrientGraphModificationException("Error on addEdge in non tx environment"), e);
      }
    }
    return edge;
  }

}
