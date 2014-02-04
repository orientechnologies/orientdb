package com.tinkerpop.blueprints.impls.orient;

import org.apache.commons.configuration.Configuration;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.tinkerpop.blueprints.Features;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public class OrientGraph extends OrientTransactionalGraph {
  protected final Features FEATURES = new Features();

  /**
   * Constructs a new object using an existent OGraphDatabase instance.
   * 
   * @param iDatabase
   *          Underlying OGraphDatabase object to attach
   */
  public OrientGraph(final ODatabaseDocumentTx iDatabase) {
    super(iDatabase);
    config();
  }

  public OrientGraph(final ODatabaseDocumentTx iDatabase, final boolean iAutoStartTx) {
	super(iDatabase, iAutoStartTx);
	config();
  }

  public OrientGraph(final String url) {
    super(url, ADMIN, ADMIN);
    config();
  }

  public OrientGraph(final String url, final boolean iAutoStartTx) {
	super(url, ADMIN, ADMIN, iAutoStartTx);
	config();
  }

  public OrientGraph(final String url, final String username, final String password) {
    super(url, username, password);
    config();
  }

  public OrientGraph(final String url, final String username, final String password, final boolean iAutoStartTx) {
    super(url, username, password, iAutoStartTx);
    config();
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
   * @param configuration
   */
  public OrientGraph(final Configuration configuration) {
    super(configuration);
    config();
  }

  public Features getFeatures() {
    // DYNAMIC FEATURES BASED ON CONFIGURATION
    FEATURES.supportsEdgeIndex = !settings.useLightweightEdges;
    FEATURES.supportsEdgeKeyIndex = !settings.useLightweightEdges;
    FEATURES.supportsEdgeIteration = !settings.useLightweightEdges;
    FEATURES.supportsEdgeRetrieval = !settings.useLightweightEdges;
    return FEATURES;
  }

  protected void config() {
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
  }
}
