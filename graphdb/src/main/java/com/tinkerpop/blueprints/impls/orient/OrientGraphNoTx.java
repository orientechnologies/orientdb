package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import org.apache.commons.configuration.Configuration;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
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
    super(iDatabase, null, null);
    config();
  }

  public OrientGraphNoTx(OPartitionedDatabasePool pool) {
    super(pool);
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
    super(iDatabase, user, password);
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
  }
}
