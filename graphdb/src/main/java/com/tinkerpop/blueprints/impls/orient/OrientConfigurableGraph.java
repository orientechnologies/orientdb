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

import com.orientechnologies.orient.core.intent.OIntent;

/**
 * Base class to manage graph settings.
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientConfigurableGraph {
  protected Settings settings = new Settings();

  protected static final boolean     USE_LIGHTWEIGHT_EDGES_DEFAULT                    = false;
  protected static final boolean     USE_CLASS_FOR_EDGE_LABEL_DEFAULT                 = true;
  protected static final boolean     USE_CLASS_FOR_VERTEX_LABEL_DEFAULT               = true;
  protected static final boolean     KEEP_IN_MEMORY_REFERENCES_DEFAULT                = false;
  protected static final boolean     USE_VERTEX_FIELDS_FOR_EDGE_LABELS                = true;
  protected static final boolean     SAVE_ORIGINAL_IDS_DEFAULT                        = false;
  protected static final boolean     STANDARD_ELEMENT_CONSTRAINTS_DEFAULT             = true;
  protected static final boolean     STANDARD_EXCEPTIONS                              = false;
  protected static final boolean     WARN_ON_FORCE_CLOSING_TX_DEFAULT                 = true;
  protected static final boolean     AUTO_SCALE_EDGE_TYPE_DEFAULT                     = false;
  protected static final boolean     USE_LOG_DEFAULT                                  = true;
  protected static final int         EDGE_CONTAINER_EMBEDDED_2_TREE_THRESHOLD_DEFAULT = -1;
  protected static final int         EDGE_CONTAINER_TREE_2_EMBEDDED_THRESHOLD_DEFAULT = -1;
  protected static final THREAD_MODE THREAD_MODE_DEFAULT                              = THREAD_MODE.AUTOSET_IFNULL;
  protected static final boolean     AUTO_START_TX_DEFAULT                            = true;
  protected static final boolean     REQUIRE_TRANSACTION_DEFAULT                      = false;
  protected static final int         STANDARD_MAX_RETRIES                             = 50;

  public enum THREAD_MODE {
    MANUAL, AUTOSET_IFNULL, ALWAYS_AUTOSET
  }

  public static class Settings {

    private Boolean     useLightweightEdges                 = null;
    private Boolean     useClassForEdgeLabel                = null;
    private Boolean     useClassForVertexLabel              = null;
    private Boolean     keepInMemoryReferences              = null;
    private Boolean     useVertexFieldsForEdgeLabels        = null;
    private Boolean     saveOriginalIds                     = null;
    private Boolean     standardElementConstraints          = null;
    private Boolean     standardExceptions                  = null;
    private Boolean     warnOnForceClosingTx                = null;
    private Boolean     autoScaleEdgeType                   = null;
    private Integer     edgeContainerEmbedded2TreeThreshold = null;
    private Integer     edgeContainerTree2EmbeddedThreshold = null;
    private THREAD_MODE threadMode                          = null;
    private Boolean     autoStartTx                         = null;
    private Boolean     requireTransaction                  = null;
    private Boolean     useLog                              = null;
    private Integer     maxRetries                          = null;

    public Settings copy() {
      final Settings copy = new Settings();
      copy.useLightweightEdges = useLightweightEdges;
      copy.useClassForEdgeLabel = useClassForEdgeLabel;
      copy.useClassForVertexLabel = useClassForVertexLabel;
      copy.keepInMemoryReferences = keepInMemoryReferences;
      copy.useVertexFieldsForEdgeLabels = useVertexFieldsForEdgeLabels;
      copy.saveOriginalIds = saveOriginalIds;
      copy.standardElementConstraints = standardElementConstraints;
      copy.standardExceptions = standardExceptions;
      copy.warnOnForceClosingTx = warnOnForceClosingTx;
      copy.autoScaleEdgeType = autoScaleEdgeType;
      copy.edgeContainerEmbedded2TreeThreshold = edgeContainerEmbedded2TreeThreshold;
      copy.edgeContainerTree2EmbeddedThreshold = edgeContainerTree2EmbeddedThreshold;
      copy.threadMode = threadMode;
      copy.autoStartTx = autoStartTx;
      copy.requireTransaction = requireTransaction;
      copy.useLog = useLog;
      copy.maxRetries = maxRetries;
      return copy;
    }

    /**
     * copies only not null settings from the input settings object
     * 
     * @param settings
     */
    public void copyFrom(final Settings settings) {
      if (settings.useLightweightEdges != null) {
        useLightweightEdges = settings.useLightweightEdges;
      }
      if (settings.useClassForEdgeLabel != null) {
        useClassForEdgeLabel = settings.useClassForEdgeLabel;
      }
      if (settings.useClassForVertexLabel != null) {
        useClassForVertexLabel = settings.useClassForVertexLabel;
      }
      if (settings.keepInMemoryReferences != null) {
        keepInMemoryReferences = settings.keepInMemoryReferences;
      }
      if (settings.useVertexFieldsForEdgeLabels != null) {
        useVertexFieldsForEdgeLabels = settings.useVertexFieldsForEdgeLabels;
      }
      if (settings.saveOriginalIds != null) {
        saveOriginalIds = settings.saveOriginalIds;
      }
      if (settings.standardElementConstraints != null) {
        standardElementConstraints = settings.standardElementConstraints;
      }
      if (settings.standardExceptions != null) {
        standardExceptions = settings.standardExceptions;
      }
      if (settings.warnOnForceClosingTx != null) {
        warnOnForceClosingTx = settings.warnOnForceClosingTx;
      }
      if (settings.autoScaleEdgeType != null) {
        autoScaleEdgeType = settings.autoScaleEdgeType;
      }
      if (settings.edgeContainerEmbedded2TreeThreshold != null) {
        edgeContainerEmbedded2TreeThreshold = settings.edgeContainerEmbedded2TreeThreshold;
      }
      if (settings.edgeContainerTree2EmbeddedThreshold != null) {
        edgeContainerTree2EmbeddedThreshold = settings.edgeContainerTree2EmbeddedThreshold;
      }
      if (settings.threadMode != null) {
        threadMode = settings.threadMode;
      }
      if (settings.autoStartTx != null) {
        autoStartTx = settings.autoStartTx;
      }
      if (settings.requireTransaction != null) {
        requireTransaction = settings.requireTransaction;
      }
      if (settings.useLog != null) {
        useLog = settings.useLog;
      }
    }

    /**
     * Returns true if is using lightweight edges, otherwise false.
     */
    public boolean isUseLightweightEdges() {
      if (useLightweightEdges == null) {
        return USE_LIGHTWEIGHT_EDGES_DEFAULT;
      }
      return useLightweightEdges;
    }

    /**
     * Changes the setting about usage of lightweight edges.
     */
    public void setUseLightweightEdges(final boolean useDynamicEdges) {
      useLightweightEdges = useDynamicEdges;
    }

    /**
     * Returns true if is using auto scale edge type, otherwise false.
     */
    public boolean isAutoScaleEdgeType() {
      if (autoScaleEdgeType == null) {
        return AUTO_SCALE_EDGE_TYPE_DEFAULT;
      }

      return autoScaleEdgeType;
    }

    /**
     * Changes the setting about usage of auto scale edge type.
     */
    public void setAutoScaleEdgeType(final boolean autoScaleEdgeType) {
      this.autoScaleEdgeType = autoScaleEdgeType;

    }

    /**
     * Returns true if is using transaction logs.
     */
    public boolean isUseLog() {
      if (useLog == null) {
        return USE_LOG_DEFAULT;
      }

      return useLog;
    }

    /**
     * Changes the setting about usage of transaction log.
     */
    public void setUseLog(final boolean useLog) {
      this.useLog = useLog;

    }

    /**
     * Returns the maximum number of retry in case of auto managed OConcurrentModificationException (like addEdge).
     */
    public int getMaxRetries() {
      if (maxRetries == null) {
        return STANDARD_MAX_RETRIES;
      }
      return maxRetries;
    }

    /**
     * Changes the maximum number of retry in case of auto managed OConcurrentModificationException (like addEdge).
     */
    public void setMaxRetries(final int maxRetries) {
      this.maxRetries = maxRetries;
    }

    /**
     * Returns the minimum number of edges for edge containers to transform the underlying structure from embedded to tree.
     */
    public int getEdgeContainerEmbedded2TreeThreshold() {
      if (edgeContainerEmbedded2TreeThreshold == null) {
        return EDGE_CONTAINER_EMBEDDED_2_TREE_THRESHOLD_DEFAULT;
      }
      return edgeContainerEmbedded2TreeThreshold;
    }

    /**
     * Changes the minimum number of edges for edge containers to transform the underlying structure from embedded to tree. Use -1
     * to disable transformation.
     */
    public void setEdgeContainerEmbedded2TreeThreshold(final int edgeContainerEmbedded2TreeThreshold) {
      this.edgeContainerEmbedded2TreeThreshold = edgeContainerEmbedded2TreeThreshold;
    }

    /**
     * Returns the minimum number of edges for edge containers to transform the underlying structure from tree to embedded.
     */
    public int getEdgeContainerTree2EmbeddedThreshold() {
      if (edgeContainerTree2EmbeddedThreshold == null) {
        return EDGE_CONTAINER_TREE_2_EMBEDDED_THRESHOLD_DEFAULT;
      }
      return edgeContainerTree2EmbeddedThreshold;
    }

    /**
     * Changes the minimum number of edges for edge containers to transform the underlying structure from tree to embedded. Use -1
     * to disable transformation.
     */
    public void setEdgeContainerTree2EmbeddedThreshold(final int edgeContainerTree2EmbeddedThreshold) {
      this.edgeContainerTree2EmbeddedThreshold = edgeContainerTree2EmbeddedThreshold;
    }

    /**
     * Tells if a transaction is started automatically when the graph is changed. This affects only when a transaction hasn't been
     * started. Default is true.
     *
     * @return
     */
    public boolean isAutoStartTx() {
      if (autoStartTx == null) {
        return AUTO_START_TX_DEFAULT;
      }
      return autoStartTx;
    }

    /**
     * If enabled auto starts a new transaction right before the graph is changed. This affects only when a transaction hasn't been
     * started. Default is true.
     *
     * @param autoStartTx
     */
    public void setAutoStartTx(final boolean autoStartTx) {
      this.autoStartTx = autoStartTx;
    }

    public boolean isRequireTransaction() {
      if (requireTransaction == null) {
        return REQUIRE_TRANSACTION_DEFAULT;
      }
      return requireTransaction;
    }

    public void setRequireTransaction(final boolean requireTransaction) {
      this.requireTransaction = requireTransaction;
    }

    /**
     * Returns true if it saves the original Id, otherwise false.
     */
    public boolean isSaveOriginalIds() {
      if (saveOriginalIds == null) {
        return SAVE_ORIGINAL_IDS_DEFAULT;
      }
      return saveOriginalIds;
    }

    /**
     * Changes the setting about usage of lightweight edges.
     */
    public void setSaveOriginalIds(final boolean saveIds) {
      saveOriginalIds = saveIds;
    }

    /**
     * Returns true if the references are kept in memory.
     */
    public boolean isKeepInMemoryReferences() {
      if (keepInMemoryReferences == null) {
        return KEEP_IN_MEMORY_REFERENCES_DEFAULT;
      }
      return keepInMemoryReferences;
    }

    /**
     * Changes the setting about using references in memory.
     */
    public void setKeepInMemoryReferences(boolean useReferences) {
      keepInMemoryReferences = useReferences;
    }

    /**
     * Returns true if the class are use for Edge labels.
     */
    public boolean isUseClassForEdgeLabel() {
      if (useClassForEdgeLabel == null) {
        return USE_CLASS_FOR_EDGE_LABEL_DEFAULT;
      }
      return useClassForEdgeLabel;
    }

    /**
     * Changes the setting to use the Edge class for Edge labels.
     */
    public void setUseClassForEdgeLabel(final boolean useCustomClassesForEdges) {
      useClassForEdgeLabel = useCustomClassesForEdges;
    }

    /**
     * Returns true if the class are use for Vertex labels.
     */
    public boolean isUseClassForVertexLabel() {
      if (useClassForVertexLabel == null) {
        return USE_CLASS_FOR_VERTEX_LABEL_DEFAULT;
      }
      return useClassForVertexLabel;
    }

    /**
     * Changes the setting to use the Vertex class for Vertex labels.
     */
    public void setUseClassForVertexLabel(final boolean useCustomClassesForVertex) {
      this.useClassForVertexLabel = useCustomClassesForVertex;
    }

    /**
     * Returns true if the out/in fields in vertex are post-fixed with edge labels. This improves traversal time by partitioning
     * edges on different collections, one per Edge's class.
     */
    public boolean isUseVertexFieldsForEdgeLabels() {
      if (useVertexFieldsForEdgeLabels == null) {
        return USE_VERTEX_FIELDS_FOR_EDGE_LABELS;
      }
      return useVertexFieldsForEdgeLabels;
    }

    /**
     * Changes the setting to postfix vertices fields with edge labels. This improves traversal time by partitioning edges on
     * different collections, one per Edge's class.
     */
    public void setUseVertexFieldsForEdgeLabels(final boolean useVertexFieldsForEdgeLabels) {
      this.useVertexFieldsForEdgeLabels = useVertexFieldsForEdgeLabels;
    }

    /**
     * Returns true if Blueprints standard exceptions are used:
     * <li>
     * <ul>
     * IllegalStateException instead of ORecordNotFoundException when the record was not found
     * </ul>
     * </li>
     */
    public boolean isStandardElementConstraints() {
      if (standardElementConstraints == null) {
        return STANDARD_ELEMENT_CONSTRAINTS_DEFAULT;
      }
      return standardElementConstraints;
    }

    /**
     * Changes the setting to apply the Blueprints standard constraints against elements.
     */
    public void setStandardElementConstraints(final boolean allowsPropertyValueNull) {
      this.standardElementConstraints = allowsPropertyValueNull;
    }

    /**
     * Returns true if the warning is generated on force the graph closing.
     */
    public boolean isStandardExceptions() {
      if (standardExceptions == null) {
        return STANDARD_EXCEPTIONS;
      }
      return standardExceptions;
    }

    /**
     * Changes the setting to throw Blueprints standard exceptions:
     * <li>
     * <ul>
     * IllegalStateException instead of ORecordNotFoundException when the record was not found
     * </ul>
     * </li>
     */
    public void setStandardExceptions(final boolean stdExceptions) {
      this.standardExceptions = stdExceptions;
    }

    /**
     * Returns true if the warning is generated on force the graph closing.
     */
    public boolean isWarnOnForceClosingTx() {
      if (warnOnForceClosingTx == null) {
        return WARN_ON_FORCE_CLOSING_TX_DEFAULT;
      }
      return warnOnForceClosingTx;
    }

    /**
     * Changes the setting to generate a warning if the graph closing has been forced.
     */
    public void setWarnOnForceClosingTx(final boolean warnOnSchemaChangeInTx) {
      this.warnOnForceClosingTx = warnOnSchemaChangeInTx;
    }

    /**
     * Returns the current thread mode:
     * <ul>
     * <li><b>MANUAL</b> the user has to manually invoke the current database in Thread Local:
     * ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());</li>
     * <li><b>AUTOSET_IFNULL</b> (default) each call assures the current graph instance is set in the Thread Local only if no one
     * was set before</li>
     * <li><b>ALWAYS_AUTOSET</b> each call assures the current graph instance is set in the Thread Local</li>
     * </ul>
     *
     * @see #setThreadMode(THREAD_MODE)
     * @return Current Graph instance to allow calls in chain (fluent interface)
     */

    public THREAD_MODE getThreadMode() {
      if (threadMode == null) {
        return THREAD_MODE_DEFAULT;
      }
      return threadMode;
    }

    /**
     * Changes the thread mode:
     * <ul>
     * <li><b>MANUAL</b> the user has to manually invoke the current database in Thread Local:
     * ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());</li>
     * <li><b>AUTOSET_IFNULL</b> (default) each call assures the current graph instance is set in the Thread Local only if no one
     * was set before</li>
     * <li><b>ALWAYS_AUTOSET</b> each call assures the current graph instance is set in the Thread Local</li>
     * </ul>
     *
     * @param iControl
     *          Value to set
     * @see #getThreadMode()
     * @return Current Graph instance to allow calls in chain (fluent interface)
     */
    public void setThreadMode(final THREAD_MODE iControl) {
      this.threadMode = iControl;
    }
  }

  protected OrientConfigurableGraph() {
  }

  public abstract void declareIntent(OIntent iIntent);

  /**
   * Returns true if is using lightweight edges, otherwise false.
   */
  public boolean isUseLightweightEdges() {
    return settings.isUseLightweightEdges();
  }

  /**
   * Changes the setting about usage of lightweight edges.
   */
  public OrientConfigurableGraph setUseLightweightEdges(final boolean useDynamicEdges) {
    settings.setUseLightweightEdges(useDynamicEdges);
    return this;
  }

  /**
   * Returns true if is using auto scale edge type, otherwise false.
   */
  public boolean isAutoScaleEdgeType() {
    return settings.isAutoScaleEdgeType();
  }

  /**
   * Changes the setting about usage of auto scale edge type.
   */
  public OrientConfigurableGraph setAutoScaleEdgeType(final boolean autoScaleEdgeType) {
    settings.setAutoScaleEdgeType(autoScaleEdgeType);
    return this;
  }

  /**
   * Returns the minimum number of edges for edge containers to transform the underlying structure from embedded to tree.
   */
  public int getEdgeContainerEmbedded2TreeThreshold() {
    return settings.getEdgeContainerEmbedded2TreeThreshold();
  }

  /**
   * Changes the minimum number of edges for edge containers to transform the underlying structure from embedded to tree. Use -1 to
   * disable transformation.
   */
  public OrientConfigurableGraph setEdgeContainerEmbedded2TreeThreshold(final int edgeContainerEmbedded2TreeThreshold) {
    this.settings.setEdgeContainerEmbedded2TreeThreshold(edgeContainerEmbedded2TreeThreshold);
    return this;
  }

  /**
   * Returns the minimum number of edges for edge containers to transform the underlying structure from tree to embedded.
   */
  public int getEdgeContainerTree2EmbeddedThreshold() {
    return settings.getEdgeContainerTree2EmbeddedThreshold();
  }

  /**
   * Changes the minimum number of edges for edge containers to transform the underlying structure from tree to embedded. Use -1 to
   * disable transformation.
   */
  public OrientConfigurableGraph setEdgeContainerTree2EmbeddedThreshold(final int edgeContainerTree2EmbeddedThreshold) {
    this.settings.edgeContainerTree2EmbeddedThreshold = edgeContainerTree2EmbeddedThreshold;
    return this;
  }

  /**
   * Tells if a transaction is started automatically when the graph is changed. This affects only when a transaction hasn't been
   * started. Default is true.
   *
   * @return
   */
  public boolean isAutoStartTx() {
    return settings.isAutoStartTx();
  }

  /**
   * If enabled auto starts a new transaction right before the graph is changed. This affects only when a transaction hasn't been
   * started. Default is true.
   *
   * @param autoStartTx
   */
  public void setAutoStartTx(final boolean autoStartTx) {
    this.settings.setAutoStartTx(autoStartTx);
  }

  public boolean isRequireTransaction() {
    return settings.isRequireTransaction();
  }

  public void setRequireTransaction(final boolean requireTransaction) {
    this.settings.setRequireTransaction(requireTransaction);
  }

  /**
   * Returns true if it saves the original Id, otherwise false.
   */
  public boolean isSaveOriginalIds() {
    return settings.isSaveOriginalIds();
  }

  /**
   * Changes the setting about usage of lightweight edges.
   */
  public OrientConfigurableGraph setSaveOriginalIds(final boolean saveIds) {
    settings.setSaveOriginalIds(saveIds);
    return this;
  }

  /**
   * Returns true if the references are kept in memory.
   */
  public boolean isKeepInMemoryReferences() {
    return settings.isKeepInMemoryReferences();
  }

  /**
   * Changes the setting about using references in memory.
   */
  public OrientConfigurableGraph setKeepInMemoryReferences(boolean useReferences) {
    settings.setKeepInMemoryReferences(useReferences);
    return this;
  }

  /**
   * Returns true if the class are use for Edge labels.
   */
  public boolean isUseClassForEdgeLabel() {
    return settings.isUseClassForEdgeLabel();
  }

  /**
   * Changes the setting to use the Edge class for Edge labels.
   */
  public OrientConfigurableGraph setUseClassForEdgeLabel(final boolean useCustomClassesForEdges) {
    settings.setUseClassForEdgeLabel(useCustomClassesForEdges);
    return this;
  }

  /**
   * Returns true if the class are use for Vertex labels.
   */
  public boolean isUseClassForVertexLabel() {
    return settings.isUseClassForVertexLabel();
  }

  /**
   * Changes the setting to use the Vertex class for Vertex labels.
   */
  public OrientConfigurableGraph setUseClassForVertexLabel(final boolean useCustomClassesForVertex) {
    this.settings.setUseClassForVertexLabel(useCustomClassesForVertex);
    return this;
  }

  /**
   * Returns true if the out/in fields in vertex are post-fixed with edge labels. This improves traversal time by partitioning edges
   * on different collections, one per Edge's class.
   */
  public boolean isUseVertexFieldsForEdgeLabels() {
    return settings.isUseVertexFieldsForEdgeLabels();
  }

  /**
   * Changes the setting to postfix vertices fields with edge labels. This improves traversal time by partitioning edges on
   * different collections, one per Edge's class.
   */
  public OrientConfigurableGraph setUseVertexFieldsForEdgeLabels(final boolean useVertexFieldsForEdgeLabels) {
    this.settings.setUseVertexFieldsForEdgeLabels(useVertexFieldsForEdgeLabels);
    return this;
  }

  /**
   * Returns true if Blueprints standard constraints are applied to elements.
   */
  public boolean isStandardElementConstraints() {
    return settings.isStandardElementConstraints();
  }

  /**
   * Changes the setting to apply the Blueprints standard constraints against elements.
   */
  public OrientConfigurableGraph setStandardElementConstraints(final boolean allowsPropertyValueNull) {
    this.settings.setStandardElementConstraints(allowsPropertyValueNull);
    return this;
  }

  /**
   * Returns true if Blueprints standard exceptions are used:
   * <li>
   * <ul>
   * IllegalStateException instead of ORecordNotFoundException when the record was not found
   * </ul>
   * </li>
   */
  public boolean isStandardExceptions() {
    return settings.isStandardExceptions();
  }

  /**
   * Changes the setting to throw Blueprints standard exceptions:
   * <li>
   * <ul>
   * IllegalStateException instead of ORecordNotFoundException when the record was not found
   * </ul>
   * </li>
   */
  public OrientConfigurableGraph setStandardExceptions(final boolean stdExceptions) {
    this.settings.setStandardExceptions(stdExceptions);
    return this;
  }

  /**
   * Returns true if the warning is generated on force the graph closing.
   */
  public boolean isWarnOnForceClosingTx() {
    return settings.isWarnOnForceClosingTx();
  }

  /**
   * Changes the setting to generate a warning if the graph closing has been forced.
   */
  public OrientConfigurableGraph setWarnOnForceClosingTx(final boolean warnOnSchemaChangeInTx) {
    this.settings.setWarnOnForceClosingTx(warnOnSchemaChangeInTx);
    return this;
  }

  /**
   * Returns the current thread mode:
   * <ul>
   * <li><b>MANUAL</b> the user has to manually invoke the current database in Thread Local:
   * ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());</li>
   * <li><b>AUTOSET_IFNULL</b> (default) each call assures the current graph instance is set in the Thread Local only if no one was
   * set before</li>
   * <li><b>ALWAYS_AUTOSET</b> each call assures the current graph instance is set in the Thread Local</li>
   * </ul>
   * 
   * @see #setThreadMode(THREAD_MODE)
   * @return Current Graph instance to allow calls in chain (fluent interface)
   */

  public THREAD_MODE getThreadMode() {
    return settings.getThreadMode();
  }

  /**
   * Changes the thread mode:
   * <ul>
   * <li><b>MANUAL</b> the user has to manually invoke the current database in Thread Local:
   * ODatabaseRecordThreadLocal.INSTANCE.set(graph.getRawGraph());</li>
   * <li><b>AUTOSET_IFNULL</b> (default) each call assures the current graph instance is set in the Thread Local only if no one was
   * set before</li>
   * <li><b>ALWAYS_AUTOSET</b> each call assures the current graph instance is set in the Thread Local</li>
   * </ul>
   * 
   * @param iControl
   *          Value to set
   * @see #getThreadMode()
   * @return Current Graph instance to allow calls in chain (fluent interface)
   */
  public OrientConfigurableGraph setThreadMode(final THREAD_MODE iControl) {
    this.settings.setThreadMode(iControl);
    return this;
  }

  public OrientConfigurableGraph setUseLog(final boolean useLog) {
    this.settings.useLog = useLog;
    return this;
  }

  /**
   * Returns the maximum number of retries in case of auto managed OConcurrentModificationException (like addEdge).
   */
  public int getMaxRetries() {
    return this.settings.getMaxRetries();
  }

  /**
   * Changes the maximum number of retries in case of auto managed OConcurrentModificationException (like addEdge).
   */
  public void setMaxRetries(final int maxRetries) {
    this.settings.setMaxRetries(maxRetries);
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
   * <td>Saves the original element IDs by using the property origId. This could be useful on import of graph to preserve original
   * ids</td>
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
   * <td>blueprints.orientdb.autoScaleEdgeType</td>
   * <td>Set auto scale of edge type. True means one edge is managed as LINK, 2 or more are managed with a LINKBAG</td>
   * <td>false</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.edgeContainerEmbedded2TreeThreshold</td>
   * <td>Changes the minimum number of edges for edge containers to transform the underlying structure from embedded to tree. Use -1
   * to disable transformation</td>
   * <td>-1</td>
   * </tr>
   * <tr>
   * <td>blueprints.orientdb.edgeContainerTree2EmbeddedThreshold</td>
   * <td>Changes the minimum number of edges for edge containers to transform the underlying structure from tree to embedded. Use -1
   * to disable transformation</td>
   * <td>-1</td>
   * </tr>
   * </table>
   *
   * @param configuration
   *          of graph
   */
  protected void init(final Configuration configuration) {
    final Boolean saveOriginalIds = configuration.getBoolean("blueprints.orientdb.saveOriginalIds", null);
    if (saveOriginalIds != null)
      setSaveOriginalIds(saveOriginalIds);

    final Boolean keepInMemoryReferences = configuration.getBoolean("blueprints.orientdb.keepInMemoryReferences", null);
    if (keepInMemoryReferences != null)
      setKeepInMemoryReferences(keepInMemoryReferences);

    final Boolean useCustomClassesForEdges = configuration.getBoolean("blueprints.orientdb.useCustomClassesForEdges", null);
    if (useCustomClassesForEdges != null)
      setUseClassForEdgeLabel(useCustomClassesForEdges);

    final Boolean useCustomClassesForVertex = configuration.getBoolean("blueprints.orientdb.useCustomClassesForVertex", null);
    if (useCustomClassesForVertex != null)
      setUseClassForVertexLabel(useCustomClassesForVertex);

    final Boolean useVertexFieldsForEdgeLabels = configuration.getBoolean("blueprints.orientdb.useVertexFieldsForEdgeLabels", null);
    if (useVertexFieldsForEdgeLabels != null)
      setUseVertexFieldsForEdgeLabels(useVertexFieldsForEdgeLabels);

    final Boolean lightweightEdges = configuration.getBoolean("blueprints.orientdb.lightweightEdges", null);
    if (lightweightEdges != null)
      setUseLightweightEdges(lightweightEdges);

    final Boolean autoScaleEdgeType = configuration.getBoolean("blueprints.orientdb.autoScaleEdgeType", null);
    if (autoScaleEdgeType != null)
      setAutoScaleEdgeType(autoScaleEdgeType);

    final Boolean requireTransaction = configuration.getBoolean("blueprints.orientdb.requireTransaction", null);
    if (requireTransaction != null)
      setRequireTransaction(requireTransaction);
  }
}
