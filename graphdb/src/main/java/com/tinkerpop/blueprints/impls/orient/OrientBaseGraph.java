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

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.conflict.ORecordConflictStrategy;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.OPartitionedDatabasePool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.intent.OIntent;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.Parameter;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.util.ExceptionFactory;
import com.tinkerpop.blueprints.util.StringFactory;
import com.tinkerpop.blueprints.util.wrappers.partition.PartitionVertex;
import org.apache.commons.configuration.Configuration;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 *
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientBaseGraph extends OrientConfigurableGraph implements OrientExtendedGraph {
  public static final String                                  CONNECTION_OUT      = "out";
  public static final String                                  CONNECTION_IN       = "in";
  public static final String                                  CLASS_PREFIX        = "class:";
  public static final String                                  CLUSTER_PREFIX      = "cluster:";
  public static final String                                  ADMIN               = "admin";
  private static volatile ThreadLocal<OrientBaseGraph>        activeGraph         = new ThreadLocal<OrientBaseGraph>();
  private static volatile ThreadLocal<Deque<OrientBaseGraph>> initializationStack = new InitializationStackThreadLocal();

  static {
    Orient.instance().registerListener(new OOrientListenerAbstract() {
      @Override
      public void onStartup() {
        if (activeGraph == null)
          activeGraph = new ThreadLocal<OrientBaseGraph>();
        if (initializationStack == null)
          initializationStack = new InitializationStackThreadLocal();
      }

      @Override
      public void onShutdown() {
        activeGraph = null;
        initializationStack = null;
      }
    });
  }

  private final OPartitionedDatabasePool                      pool;
  protected ODatabaseDocumentTx                               database;
  private String                                              url;
  private String                                              username;
  private String                                              password;

  /**
   * Constructs a new object using an existent database instance.
   *
   * @param iDatabase
   *          Underlying database object to attach
   */
  public OrientBaseGraph(final ODatabaseDocumentTx iDatabase, final String iUserName, final String iUserPassword,
      final Settings iConfiguration) {
    this.pool = null;
    this.username = iUserName;
    this.password = iUserPassword;

    database = iDatabase;
    checkForGraphSchema(database);

    makeActive();
    putInInitializationStack();

    readDatabaseConfiguration();
    configure(iConfiguration);
  }

  public OrientBaseGraph(final OPartitionedDatabasePool pool) {
    this.pool = pool;

    database = pool.acquire();
    makeActive();
    putInInitializationStack();

    this.username = database.getUser() != null ? database.getUser().getName() : null;

    readDatabaseConfiguration();
  }

  public OrientBaseGraph(final OPartitionedDatabasePool pool, final Settings iConfiguration) {
    this.pool = pool;

    database = pool.acquire();
    makeActive();
    putInInitializationStack();
    this.username = database.getUser() != null ? database.getUser().getName() : null;

    readDatabaseConfiguration();
    configure(iConfiguration);
  }

  public OrientBaseGraph(final String url) {
    this(url, ADMIN, ADMIN);
  }

  public OrientBaseGraph(final String url, final String username, final String password) {
    this.pool = null;
    this.url = OFileUtils.getPath(url);
    this.username = username;
    this.password = password;
    this.openOrCreate();
    readDatabaseConfiguration();
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
  public OrientBaseGraph(final Configuration configuration) {
    this(configuration.getString("blueprints.orientdb.url", null), configuration.getString("blueprints.orientdb.username", null),
        configuration.getString("blueprints.orientdb.password", null));
    super.init(configuration);
  }

  public static OrientBaseGraph getActiveGraph() {
    return activeGraph.get();
  }

  /**
   * Internal use only.
   */
  public static void clearInitStack() {
    final ThreadLocal<Deque<OrientBaseGraph>> is = initializationStack;
    if (is != null)
      is.get().clear();

    final ThreadLocal<OrientBaseGraph> ag = activeGraph;
    if (ag != null)
      ag.remove();

  }

  /**
   * (Internal)
   */
  public static void encodeClassNames(final String... iLabels) {
    if (iLabels != null)
      // ENCODE LABELS
      for (int i = 0; i < iLabels.length; ++i)
        iLabels[i] = encodeClassName(iLabels[i]);
  }

  /**
   * (Internal) Returns the case sensitive edge class names.
   */
  public static void getEdgeClassNames(final OrientBaseGraph graph, final String... iLabels) {
    if (iLabels != null && graph.isUseClassForEdgeLabel()) {
      for (int i = 0; i < iLabels.length; ++i) {
        final OrientEdgeType edgeType = graph.getEdgeType(iLabels[i]);
        if (edgeType != null)
          // OVERWRITE CLASS NAME BECAUSE ATTRIBUTES ARE CASE SENSITIVE
          iLabels[i] = edgeType.getName();
      }
    }
  }

  /**
   * (Internal)
   */
  public static String encodeClassName(String iClassName) {
    if (iClassName == null)
      return null;

    if (Character.isDigit(iClassName.charAt(0)))
      iClassName = "-" + iClassName;

    try {
      return URLEncoder.encode(iClassName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      OLogManager.instance().error(null, "Error on encoding class name using encoding '%s'", e, "UTF-8");
      return iClassName;
    }
  }

  /**
   * (Internal)
   */
  public static String decodeClassName(String iClassName) {
    if (iClassName == null)
      return null;

    if (iClassName.charAt(0) == '-')
      iClassName = iClassName.substring(1);

    try {
      return URLDecoder.decode(iClassName, "UTF-8");
    } catch (UnsupportedEncodingException e) {
      OLogManager.instance().error(null, "Error on decoding class name using encoding '%s'", e, "UTF-8");
      return iClassName;
    }
  }

  protected static void checkForGraphSchema(final ODatabaseDocumentTx iDatabase) {
    final OSchema schema = iDatabase.getMetadata().getSchema();

    final OClass vertexBaseClass = schema.getClass(OrientVertexType.CLASS_NAME);
    final OClass edgeBaseClass = schema.getClass(OrientEdgeType.CLASS_NAME);

    if (vertexBaseClass == null)
      // CREATE THE META MODEL USING THE ORIENT SCHEMA
      schema.createClass(OrientVertexType.CLASS_NAME).setOverSize(2);

    if (edgeBaseClass == null)
      schema.createClass(OrientEdgeType.CLASS_NAME);

    // @COMPATIBILITY < 1.4.0:
    boolean warn = false;
    final String MSG_SUFFIX = ". Probably you are using a database created with a previous version of OrientDB. Export in graphml format and reimport it";

    if (vertexBaseClass != null) {
      if (!vertexBaseClass.getName().equals(OrientVertexType.CLASS_NAME)) {
        OLogManager.instance().warn(null, "Found Vertex class %s" + MSG_SUFFIX, vertexBaseClass.getName());
        warn = true;
      }

      if (vertexBaseClass.existsProperty(CONNECTION_OUT) || vertexBaseClass.existsProperty(CONNECTION_IN)) {
        OLogManager.instance().warn(null, "Found property in/out against V");
        warn = true;
      }
    }

    if (edgeBaseClass != null) {
      if (!warn && !edgeBaseClass.getName().equals(OrientEdgeType.CLASS_NAME)) {
        OLogManager.instance().warn(null, "Found Edge class %s" + MSG_SUFFIX, edgeBaseClass.getName());
        warn = true;
      }

      if (edgeBaseClass.existsProperty(CONNECTION_OUT) || edgeBaseClass.existsProperty(CONNECTION_IN)) {
        OLogManager.instance().warn(null, "Found property in/out against E");
        warn = true;
      }
    }
  }

  public void makeActive() {
    activeGraph.set(this);

    final ODatabaseDocument tlDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (database != null && tlDb != database)
      ODatabaseRecordThreadLocal.INSTANCE.set(database);
  }

  /**
   * (Blueprints Extension) Configure the Graph instance.
   *
   * @param iSetting
   *          Settings object containing all the settings
   */
  public OrientBaseGraph configure(final Settings iSetting) {
    makeActive();

    if (iSetting != null) {
      if (settings == null) {
        settings = iSetting;
      } else {
        settings.copyFrom(iSetting);
      }
    }
    return this;
  }

  /**
   * (Blueprints Extension) Drops the database
   */
  public void drop() {
    makeActive();

    getRawGraph().drop();

    pollGraphFromStack(true);
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Element> Index<T> createIndex(final String indexName, final Class<T> indexClass,
      final Parameter... indexParameters) {
    makeActive();

    return executeOutsideTx(new OCallable<Index<T>, OrientBaseGraph>() {
      public Index<T> call(final OrientBaseGraph g) {

        final OIndexManager indexManager = database.getMetadata().getIndexManager();

        if (indexManager.getIndex(indexName) != null)
          throw ExceptionFactory.indexAlreadyExists(indexName);

        final OrientIndex<? extends OrientElement> index = new OrientIndex<OrientElement>(g, indexName, indexClass, null);

        // SAVE THE CONFIGURATION INTO THE GLOBAL CONFIG
        saveIndexConfiguration();

        return (Index<T>) index;
      }
    }, "create index '", indexName, "'");
  }

  /**
   * Returns an index by name and class
   *
   * @param indexName
   *          Index name
   * @param indexClass
   *          Class as one or subclass of Vertex.class and Edge.class
   * @return Index instance
   */
  @SuppressWarnings("unchecked")
  @Override
  public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
    makeActive();

    final OIndexManager indexManager = database.getMetadata().getIndexManager();
    final OIndex idx = indexManager.getIndex(indexName);
    if (idx == null || !hasIndexClass(idx))
      return null;

    final Index<? extends Element> index = new OrientIndex(this, idx);

    if (indexClass.isAssignableFrom(index.getIndexClass()))
      return (Index<T>) index;
    else
      throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
  }

  /**
   * Returns all the indices.
   *
   * @return Iterable of Index instances
   */
  public Iterable<Index<? extends Element>> getIndices() {
    makeActive();

    return loadManualIndexes();
  }

  /**
   * Drops an index by name.
   *
   * @param indexName
   *          Index name
   */
  public void dropIndex(final String indexName) {
    makeActive();

    executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
      @Override
      public Object call(OrientBaseGraph g) {
        try {
          final OIndexManager indexManager = getRawGraph().getMetadata().getIndexManager();
          final OIndex index = indexManager.getIndex(indexName);
          final String recordMapIndexName = index.getConfiguration().field(OrientIndex.CONFIG_RECORD_MAP_NAME);

          indexManager.dropIndex(indexName);
          if (recordMapIndexName != null)
            getRawGraph().getMetadata().getIndexManager().dropIndex(recordMapIndexName);

          saveIndexConfiguration();
          return null;
        } catch (Exception e) {
          g.rollback();
          throw new RuntimeException(e.getMessage(), e);
        }
      }
    }, "drop index '", indexName, "'");
  }

  /**
   * Creates a new unconnected vertex with no fields in the Graph.
   *
   * @param id
   *          Optional, can contains the Vertex's class name by prefixing with "class:"
   * @return The new OrientVertex created
   */
  @Override
  public OrientVertex addVertex(final Object id) {
    makeActive();

    return addVertex(id, (Object[]) null);
  }

  public ORecordConflictStrategy getConflictStrategy() {
    makeActive();

    return database.getStorage().getConflictStrategy();
  }

  public OrientBaseGraph setConflictStrategy(final String iStrategyName) {
    makeActive();

    database.setConflictStrategy(Orient.instance().getRecordConflictStrategy().getStrategy(iStrategyName));
    return this;
  }

  public OrientBaseGraph setConflictStrategy(final ORecordConflictStrategy iResolver) {
    makeActive();

    database.setConflictStrategy(iResolver);
    return this;
  }

  /**
   * (Blueprints Extension) Creates a new unconnected vertex in the Graph setting the initial field values.
   *
   * @param id
   *          Optional, can contains the Vertex's class name by prefixing with "class:"
   * @param prop
   *          Fields must be a odd pairs of key/value or a single object as Map containing entries as key/value pairs
   * @return The new OrientVertex created
   */
  public OrientVertex addVertex(Object id, final Object... prop) {
    makeActive();

    String className = null;
    String clusterName = null;
    Object[] fields = null;

    if (id != null) {
      if (id instanceof String) {
        // PARSE ARGUMENTS
        final String[] args = ((String) id).split(",");
        for (String s : args) {
          if (s.startsWith(CLASS_PREFIX))
            // GET THE CLASS NAME
            className = s.substring(CLASS_PREFIX.length());
          else if (s.startsWith(CLUSTER_PREFIX))
            // GET THE CLASS NAME
            clusterName = s.substring(CLUSTER_PREFIX.length());
          else
            id = s;
        }
      }

      if (isSaveOriginalIds())
        // SAVE THE ID TOO
        fields = new Object[] { OrientElement.DEF_ORIGINAL_ID_FIELDNAME, id };
    }

    setCurrentGraphInThreadLocal();
    autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, className, fields);
    vertex.setPropertiesInternal(prop);

    // SAVE IT
    if (clusterName != null)
      vertex.save(clusterName);
    else
      vertex.save();
    return vertex;
  }

  /**
   * (Blueprints Extension) Creates a new unconnected vertex with no fields of specific class in a cluster in the Graph.
   *
   * @param iClassName
   *          Vertex class name
   * @param iClusterName
   *          Vertex cluster name
   * @return New vertex created
   */
  public OrientVertex addVertex(final String iClassName, final String iClusterName) {
    makeActive();

    setCurrentGraphInThreadLocal();
    autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, iClassName);

    // SAVE IT
    if (iClusterName != null)
      vertex.save(iClusterName);
    else
      vertex.save();
    return vertex;
  }

  /**
   * (Blueprints Extension) Creates a temporary vertex setting the initial field values. The vertex is not saved and the transaction
   * is not started.
   *
   * @param iClassName
   *          Vertex's class name
   * @param prop
   *          Fields must be a odd pairs of key/value or a single object as Map containing entries as key/value pairs
   * @return added vertex
   */
  public OrientVertex addTemporaryVertex(final String iClassName, final Object... prop) {
    makeActive();

    setCurrentGraphInThreadLocal();
    autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, iClassName);
    vertex.setPropertiesInternal(prop);
    return vertex;
  }

  /**
   * Creates an edge between a source Vertex and a destination Vertex setting label as Edge's label.
   *
   * @param id
   *          Optional, can contains the Edge's class name by prefixing with "class:"
   * @param outVertex
   *          Source vertex
   * @param inVertex
   *          Destination vertex
   * @param label
   *          Edge's label
   * @return
   */
  @Override
  public OrientEdge addEdge(final Object id, Vertex outVertex, Vertex inVertex, final String label) {
    makeActive();

    String className = null;
    String clusterName = null;

    if (id != null) {
      if (id instanceof String) {
        // PARSE ARGUMENTS
        final String[] args = ((String) id).split(",");
        for (String s : args) {
          if (s.startsWith(CLASS_PREFIX))
            // GET THE CLASS NAME
            className = s.substring(CLASS_PREFIX.length());
          else if (s.startsWith(CLUSTER_PREFIX))
            // GET THE CLASS NAME
            clusterName = s.substring(CLUSTER_PREFIX.length());
        }
      }
    }

    // SAVE THE ID TOO?
    final Object[] fields = isSaveOriginalIds() && id != null ? new Object[] { OrientElement.DEF_ORIGINAL_ID_FIELDNAME, id } : null;

    if (outVertex instanceof PartitionVertex)
      // WRAPPED: GET THE BASE VERTEX
      outVertex = ((PartitionVertex) outVertex).getBaseVertex();

    if (inVertex instanceof PartitionVertex)
      // WRAPPED: GET THE BASE VERTEX
      inVertex = ((PartitionVertex) inVertex).getBaseVertex();

    return ((OrientVertex) outVertex).addEdge(label, (OrientVertex) inVertex, className, clusterName, fields);

  }

  /**
   * Returns a vertex by an ID.
   *
   * @param id
   *          Can by a String, ODocument or an OIdentifiable object.
   */
  public OrientVertex getVertex(final Object id) {
    makeActive();

    if (null == id)
      throw ExceptionFactory.vertexIdCanNotBeNull();

    if (id instanceof OrientVertex)
      return (OrientVertex) id;
    else if (id instanceof ODocument)
      return new OrientVertex(this, (OIdentifiable) id);

    setCurrentGraphInThreadLocal();

    ORID rid;
    if (id instanceof OIdentifiable)
      rid = ((OIdentifiable) id).getIdentity();
    else {
      try {
        rid = new ORecordId(id.toString());
      } catch (IllegalArgumentException iae) {
        // orientdb throws IllegalArgumentException: Argument 'xxxx' is
        // not a RecordId in form of string. Format must be:
        // <cluster-id>:<cluster-position>
        return null;
      }
    }

    if (!rid.isValid())
      return null;

    ORecord rec = rid.getRecord();
    if (rec == null || !(rec instanceof ODocument))
      return null;

    return new OrientVertex(this, rec);
  }

  @Override
  public void declareIntent(final OIntent iIntent) {
    makeActive();

    getRawGraph().declareIntent(iIntent);
  }

  /**
   * Removes a vertex from the Graph. All the edges connected to the Vertex are automatically removed.
   *
   * @param vertex
   *          Vertex to remove
   */
  public void removeVertex(final Vertex vertex) {
    makeActive();

    vertex.remove();
  }

  /**
   * Get all the Vertices in Graph.
   *
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices() {
    makeActive();

    return getVerticesOfClass(OrientVertexType.CLASS_NAME, true);
  }

  /**
   * Get all the Vertices in Graph specifying if consider or not sub-classes of V.
   *
   * @param iPolymorphic
   *          If true then get all the vertices of any sub-class
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices(final boolean iPolymorphic) {
    makeActive();

    return getVerticesOfClass(OrientVertexType.CLASS_NAME, iPolymorphic);
  }

  /**
   * Get all the Vertices in Graph of a specific vertex class and all sub-classes.
   *
   * @param iClassName
   *          Vertex class name to filter
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVerticesOfClass(final String iClassName) {
    makeActive();

    return getVerticesOfClass(iClassName, true);
  }

  /**
   * Get all the Vertices in Graph of a specific vertex class and all sub-classes only if iPolymorphic is true.
   *
   * @param iClassName
   *          Vertex class name to filter
   * @param iPolymorphic
   *          If true consider also Vertex iClassName sub-classes
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVerticesOfClass(final String iClassName, final boolean iPolymorphic) {
    makeActive();

    final OClass cls = getRawGraph().getMetadata().getSchema().getClass(iClassName);
    if (cls == null)
      throw new IllegalArgumentException("Cannot find class '" + iClassName + "' in database schema");

    if (!cls.isSubClassOf(OrientVertexType.CLASS_NAME))
      throw new IllegalArgumentException("Class '" + iClassName + "' is not a vertex class");

    return new OrientElementScanIterable<Vertex>(this, iClassName, iPolymorphic);
  }

  /**
   * Get all the Vertices in Graph filtering by field name and value. Example:<code>
   * Iterable<Vertex> resultset = getVertices("name", "Jay");
   * </code>
   *
   * @param iKey
   *          Field name
   * @param iValue
   *          Field value
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices(final String iKey, Object iValue) {
    makeActive();

    if (iKey.equals("@class"))
      return getVerticesOfClass(iValue.toString());

    String indexName;
    final String key;
    int pos = iKey.indexOf('.');
    if (pos > -1) {
      indexName = iKey;

      final String className = iKey.substring(0, pos);
      key = iKey.substring(iKey.indexOf('.') + 1);

      final OClass clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(className);

      final Collection<? extends OIndex<?>> indexes = clazz.getIndexes();
      for (OIndex<?> index : indexes) {
        final String oInName = index.getName();
        final int point = oInName.indexOf(".");
        final String okey = oInName.substring(point + 1);
        if (okey.equals(key)) {
          indexName = oInName;
          break;
        }
      }

    } else {
      indexName = OrientVertexType.CLASS_NAME + "." + iKey;
      key = iKey;
    }

    final OIndex<?> idx = database.getMetadata().getIndexManager().getIndex(indexName);
    if (idx != null) {
      iValue = convertKey(idx, iValue);

      Object indexValue = idx.get(iValue);
      if (indexValue != null && !(indexValue instanceof Iterable<?>))
        indexValue = Arrays.asList(indexValue);

      return new OrientElementIterable<Vertex>(this, (Iterable<?>) indexValue);
    }

    // NO INDEX: EXECUTE A QUERY
    return query().has(key, iValue).vertices();
  }

  /**
   * Lookup for a vertex by id using an index.<br>
   * This API relies on Unique index (SBTREE/HASH) but is deprecated.<br>
   * Example:<code>
   * Vertex v = getVertexByKey("V.name", "name", "Jay");
   * </code>
   *
   * @param iKey
   *          Name of the indexed property
   * @param iValue
   *          Field value
   * @return Vertex instance if found, otherwise null
   * @see #getVertices(String,Object)
   */
  @Deprecated
  public Vertex getVertexByKey(final String iKey, Object iValue) {
    makeActive();

    String indexName;
    if (iKey.indexOf('.') > -1)
      indexName = iKey;
    else
      indexName = OrientVertexType.CLASS_NAME + "." + iKey;

    final OIndex<?> idx = database.getMetadata().getIndexManager().getIndex(indexName);
    if (idx != null) {
      iValue = convertKey(idx, iValue);

      Object v = idx.get(iValue);
      if (v != null)
        return getVertex(v);
      return null;
    } else
      throw new IllegalArgumentException("Index '" + indexName + "' not found");
  }

  /**
   * Get all the Vertices in Graph filtering by field name and value. Example:<code>
   * Iterable<Vertex> resultset = getVertices("Person",new String[] {"name","surname"},new Object[] { "Sherlock" ,"Holmes"});
   * </code>
   *
   * @param iKey
   *          Fields name
   * @param iValue
   *          Fields value
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices(final String label, final String[] iKey, Object[] iValue) {
    makeActive();
    final OClass clazz = database.getMetadata().getImmutableSchemaSnapshot().getClass(label);
    Set<OIndex<?>> indexes = clazz.getInvolvedIndexes(Arrays.asList(iKey));
    if (indexes.iterator().hasNext()) {
      final OIndex<?> idx = indexes.iterator().next();
      if (idx != null) {
        List<Object> keys = Arrays.asList(convertKeys(idx, iValue));
        Object key;
        if (keys.size() == 1) {
          key = keys.get(0);
        } else {
          key = new OCompositeKey(keys);
        }
        Object indexValue = idx.get(key);
        if (indexValue != null && !(indexValue instanceof Iterable<?>))
          indexValue = Arrays.asList(indexValue);
        return new OrientElementIterable<Vertex>(this, (Iterable<?>) indexValue);
      }
    }
    // NO INDEX: EXECUTE A QUERY
    OrientGraphQuery query = (OrientGraphQuery) query();
    query.labels(label);
    for (int i = 0; i < iKey.length; i++) {
      query.has(iKey[i], iValue[i]);
    }
    return query.vertices();
  }

  /**
   * Returns all the edges in Graph.
   *
   * @return Edges as Iterable
   */
  public Iterable<Edge> getEdges() {
    makeActive();

    return getEdgesOfClass(OrientEdgeType.CLASS_NAME, true);
  }

  /**
   * Get all the Edges in Graph specifying if consider or not sub-classes of E.
   *
   * @param iPolymorphic
   *          If true then get all the edge of any sub-class
   * @return Edges as Iterable
   */
  public Iterable<Edge> getEdges(final boolean iPolymorphic) {
    makeActive();

    return getEdgesOfClass(OrientEdgeType.CLASS_NAME, iPolymorphic);
  }

  /**
   * Get all the Edges in Graph of a specific edge class and all sub-classes.
   *
   * @param iClassName
   *          Edge class name to filter
   * @return Edges as Iterable
   */
  public Iterable<Edge> getEdgesOfClass(final String iClassName) {
    makeActive();

    return getEdgesOfClass(iClassName, true);
  }

  /**
   * Get all the Edges in Graph of a specific edges class and all sub-classes only if iPolymorphic is true.
   *
   * @param iClassName
   *          Edge class name to filter
   * @param iPolymorphic
   *          If true consider also iClassName Edge sub-classes
   * @return Edges as Iterable
   */
  public Iterable<Edge> getEdgesOfClass(final String iClassName, final boolean iPolymorphic) {
    makeActive();

    final OClass cls = getRawGraph().getMetadata().getSchema().getClass(iClassName);
    if (cls == null)
      throw new IllegalArgumentException("Cannot find class '" + iClassName + "' in database schema");

    if (!cls.isSubClassOf(OrientEdgeType.CLASS_NAME))
      throw new IllegalArgumentException("Class '" + iClassName + "' is not an edge class");

    return new OrientElementScanIterable<Edge>(this, iClassName, iPolymorphic);
  }

  /**
   * Get all the Edges in Graph filtering by field name and value. Example:<code>
   * Iterable<Edges> resultset = getEdges("name", "Jay");
   * </code>
   *
   * @param iKey
   *          Field name
   * @param iValue
   *          Field value
   * @return Edges as Iterable
   */
  public Iterable<Edge> getEdges(final String iKey, Object iValue) {
    makeActive();

    if (iKey.equals("@class"))
      return getEdgesOfClass(iValue.toString());

    final String indexName;
    final String key;
    int pos = iKey.indexOf('.');
    if (pos > -1) {
      indexName = iKey;
      key = iKey.substring(iKey.indexOf('.') + 1);
    } else {
      indexName = OrientEdgeType.CLASS_NAME + "." + iKey;
      key = iKey;
    }

    final OIndex<?> idx = database.getMetadata().getIndexManager().getIndex(indexName);
    if (idx != null) {
      iValue = convertKey(idx, iValue);

      Object indexValue = idx.get(iValue);
      if (indexValue != null && !(indexValue instanceof Iterable<?>))
        indexValue = Arrays.asList(indexValue);

      return new OrientElementIterable<Edge>(this, (Iterable<?>) indexValue);
    }

    // NO INDEX: EXECUTE A QUERY
    return query().has(key, iValue).edges();
  }

  /**
   * Returns a edge by an ID.
   *
   * @param id
   *          Can by a String, ODocument or an OIdentifiable object.
   */
  public OrientEdge getEdge(final Object id) {
    makeActive();

    if (null == id)
      throw ExceptionFactory.edgeIdCanNotBeNull();

    if (id instanceof OrientEdge)
      return (OrientEdge) id;
    else if (id instanceof ODocument)
      return new OrientEdge(this, (OIdentifiable) id);

    final OIdentifiable rec;
    if (id instanceof OIdentifiable)
      rec = (OIdentifiable) id;
    else {
      final String str = id.toString();

      int pos = str.indexOf("->");

      if (pos > -1) {
        // DUMMY EDGE: CREATE IT IN MEMORY
        final String from = str.substring(0, pos);
        final String to = str.substring(pos + 2);
        return new OrientEdge(this, new ORecordId(from), new ORecordId(to));
      }

      try {
        rec = new ORecordId(str);
      } catch (IllegalArgumentException iae) {
        // orientdb throws IllegalArgumentException: Argument 'xxxx' is
        // not a RecordId in form of string. Format must be:
        // [#]<cluster-id>:<cluster-position>
        return null;
      }
    }

    final ODocument doc = rec.getRecord();
    if (doc == null)
      return null;

    if (!doc.getSchemaClass().isSubClassOf(OrientEdgeType.CLASS_NAME))
      throw new IllegalArgumentException("Class '" + doc.getClassName() + "' is not an edge class");

    return new OrientEdge(this, rec);
  }

  /**
   * Removes an edge from the Graph.
   *
   * @param edge
   *          Edge to remove
   */
  public void removeEdge(final Edge edge) {
    makeActive();

    edge.remove();
  }

  /**
   * Reuses the underlying database avoiding to create and open it every time.
   *
   * @param iDatabase
   *          Underlying database object
   */
  public OrientBaseGraph reuse(final ODatabaseDocumentTx iDatabase) {
    ODatabaseRecordThreadLocal.INSTANCE.set(iDatabase);
    this.url = iDatabase.getURL();
    database = iDatabase;

    makeActive();

    return this;
  }

  /**
   * Checks if the Graph has been closed.
   *
   * @return True if it is closed, otherwise false
   */
  public boolean isClosed() {
    makeActive();

    return database == null || database.isClosed();
  }

  /**
   * Closes the Graph. After closing the Graph cannot be used.
   */
  public void shutdown() {
    shutdown(true);
  }

  /**
   * Closes the Graph. After closing the Graph cannot be used.
   */
  public void shutdown(boolean closeDb) {
    shutdown(closeDb, true);
  }

  /**
   * Closes the Graph. After closing the Graph cannot be used.
   */
  public void shutdown(boolean closeDb, boolean commitTx) {
    makeActive();

    try {
      if (!database.isClosed() && commitTx) {
        final OStorage storage = database.getStorage().getUnderlying();
        if (storage instanceof OAbstractPaginatedStorage) {
          if (((OAbstractPaginatedStorage) storage).getWALInstance() != null)
            database.commit();
        }

      }

    } catch (RuntimeException e) {
      OLogManager.instance().error(this, "Error during context close for db " + url, e);
      throw e;
    } catch (Exception e) {
      OLogManager.instance().error(this, "Error during context close for db " + url, e);
      throw new OException("Error during context close for db " + url, e);
    } finally {
      try {
        if (closeDb)
          database.close();
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during context close for db " + url, e);
      }
    }

    url = null;
    username = null;
    password = null;

    pollGraphFromStack(closeDb);

    if (!closeDb)
      database.activateOnCurrentThread();
  }

  /**
   * Returns the Graph URL.
   */
  public String toString() {
    return StringFactory.graphString(this, getRawGraph().getURL());
  }

  /**
   * Returns the underlying Database instance as ODatabaseDocumentTx instance.
   */
  public ODatabaseDocumentTx getRawGraph() {
    return database;
  }

  /**
   * Commits the current active transaction.
   */
  public void commit() {
    makeActive();
  }

  /**
   * Rollbacks the current active transaction. All the pending changes are rollbacked.
   */
  public void rollback() {
    makeActive();
  }

  /**
   * Returns the V persistent class as OrientVertexType instance.
   */
  public OrientVertexType getVertexBaseType() {
    makeActive();

    return new OrientVertexType(this, getRawGraph().getMetadata().getSchema().getClass(OrientVertexType.CLASS_NAME));
  }

  /**
   * Returns the persistent class for type iTypeName as OrientVertexType instance.
   *
   * @param iTypeName
   *          Vertex class name
   */
  public final OrientVertexType getVertexType(final String iTypeName) {
    makeActive();

    final OClass cls = getRawGraph().getMetadata().getSchema().getClass(iTypeName);
    if (cls == null)
      return null;

    OrientVertexType.checkType(cls);
    return new OrientVertexType(this, cls);

  }

  /**
   * Creates a new Vertex persistent class.
   *
   * @param iClassName
   *          Vertex class name
   * @return OrientVertexType instance representing the persistent class
   */
  public OrientVertexType createVertexType(final String iClassName) {
    makeActive();

    return createVertexType(iClassName, (String) null);
  }

  /**
   * Creates a new Vertex persistent class specifying the super class.
   *
   * @param iClassName
   *          Vertex class name
   * @param iSuperClassName
   *          Vertex class name to extend
   * @return OrientVertexType instance representing the persistent class
   */
  public OrientVertexType createVertexType(final String iClassName, final String iSuperClassName) {
    makeActive();

    return createVertexType(iClassName, iSuperClassName == null ? getVertexBaseType() : getVertexType(iSuperClassName));
  }

  /**
   * Creates a new Vertex persistent class specifying the super class.
   *
   * @param iClassName
   *          Vertex class name
   * @param iSuperClass
   *          OClass Vertex to extend
   * @return OrientVertexType instance representing the persistent class
   */
  public OrientVertexType createVertexType(final String iClassName, final OClass iSuperClass) {
    makeActive();

    OrientVertexType.checkType(iSuperClass);

    return executeOutsideTx(new OCallable<OrientVertexType, OrientBaseGraph>() {
      @Override
      public OrientVertexType call(final OrientBaseGraph g) {
        return new OrientVertexType(g, getRawGraph().getMetadata().getSchema().createClass(iClassName, iSuperClass));
      }
    }, "create vertex type '", iClassName, "' as subclass of '", iSuperClass.getName(), "'");
  }

  /**
   * Drop a vertex class.
   *
   * @param iTypeName
   *          Vertex class name
   */
  public final void dropVertexType(final String iTypeName) {
    makeActive();

    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        getRawGraph().getMetadata().getSchema().dropClass(iTypeName);
        return null;
      }
    }, "drop vertex type '", iTypeName, "'");
  }

  /**
   * Returns the E persistent class as OrientEdgeType instance.
   */
  public OrientEdgeType getEdgeBaseType() {
    makeActive();

    return new OrientEdgeType(this);
  }

  /**
   * Returns the persistent class for type iTypeName as OrientEdgeType instance.
   *
   * @param iTypeName
   *          Edge class name
   */
  public final OrientEdgeType getEdgeType(final String iTypeName) {
    makeActive();

    final OClass cls = getRawGraph().getMetadata().getSchema().getClass(iTypeName);
    if (cls == null)
      return null;

    OrientEdgeType.checkType(cls);
    return new OrientEdgeType(this, cls);
  }

  /**
   * Creates a new Edge persistent class.
   *
   * @param iClassName
   *          Edge class name
   * @return OrientEdgeType instance representing the persistent class
   */
  public OrientEdgeType createEdgeType(final String iClassName) {
    makeActive();

    return createEdgeType(iClassName, (String) null);
  }

  /**
   * Creates a new Edge persistent class specifying the super class.
   *
   * @param iClassName
   *          Edge class name
   * @param iSuperClassName
   *          Edge class name to extend
   * @return OrientEdgeType instance representing the persistent class
   */
  public OrientEdgeType createEdgeType(final String iClassName, final String iSuperClassName) {
    makeActive();

    return createEdgeType(iClassName, iSuperClassName == null ? getEdgeBaseType() : getEdgeType(iSuperClassName));
  }

  /**
   * Creates a new Edge persistent class specifying the super class.
   *
   * @param iClassName
   *          Edge class name
   * @param iSuperClass
   *          OClass Edge to extend
   * @return OrientEdgeType instance representing the persistent class
   */
  public OrientEdgeType createEdgeType(final String iClassName, final OClass iSuperClass) {
    makeActive();

    OrientEdgeType.checkType(iSuperClass);
    return executeOutsideTx(new OCallable<OrientEdgeType, OrientBaseGraph>() {
      @Override
      public OrientEdgeType call(final OrientBaseGraph g) {
        return new OrientEdgeType(g, getRawGraph().getMetadata().getSchema().createClass(iClassName, iSuperClass));
      }
    }, "create edge type '", iClassName, "' as subclass of '", iSuperClass.getName(), "'");
  }

  /**
   * Drops an edge class.
   *
   * @param iTypeName
   *          Edge class name
   */
  public final void dropEdgeType(final String iTypeName) {
    makeActive();

    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        getRawGraph().getMetadata().getSchema().dropClass(iTypeName);
        return null;
      }
    }, "drop edge type '", iTypeName, "'");
  }

  /**
   * Detaches a Graph Element to be used offline. All the changes will be committed on further @attach call.
   *
   * @param iElement
   *          Graph element to detach
   * @return The detached element
   * @see #attach(OrientElement)
   */
  public OrientElement detach(final OrientElement iElement) {
    makeActive();

    iElement.detach();
    return iElement;
  }

  /**
   * Attaches a previously detached Graph Element to the current Graph. All the pending changes will be committed.
   *
   * @param iElement
   *          Graph element to attach
   * @return The attached element
   * @see #detach(OrientElement)
   */
  public OrientElement attach(final OrientElement iElement) {
    makeActive();

    return iElement.attach(this);
  }

  /**
   * Returns a graph element, vertex or edge, starting from an ID.
   *
   * @param id
   *          Can by a String, ODocument or an OIdentifiable object.
   * @return OrientElement subclass such as OrientVertex or OrientEdge
   */
  public OrientElement getElement(final Object id) {
    makeActive();

    if (null == id)
      throw new IllegalArgumentException("id cannot be null");

    if (id instanceof OrientElement)
      return (OrientElement) id;

    OIdentifiable rec;
    if (id instanceof OIdentifiable)
      rec = (OIdentifiable) id;
    else
      try {
        rec = new ORecordId(id.toString());
      } catch (IllegalArgumentException iae) {
        // orientdb throws IllegalArgumentException: Argument 'xxxx' is
        // not a RecordId in form of string. Format must be:
        // <cluster-id>:<cluster-position>
        return null;
      }

    final ODocument doc = rec.getRecord();
    if (doc != null) {
      final OImmutableClass schemaClass = ODocumentInternal.getImmutableSchemaClass(doc);
      if (schemaClass != null && schemaClass.isEdgeType())
        return new OrientEdge(this, doc);
      else
        return new OrientVertex(this, doc);
      // else
      // throw new IllegalArgumentException("Type error. The class " + schemaClass + " does not extend class neither '"
      // + OrientVertexType.CLASS_NAME + "' nor '" + OrientEdgeType.CLASS_NAME + "'");
    }

    return null;
  }

  /**
   * Drops the index against a field name.
   *
   * @param key
   *          Field name
   * @param elementClass
   *          Element class as instances of Vertex and Edge
   */
  public <T extends Element> void dropKeyIndex(final String key, final Class<T> elementClass) {
    makeActive();

    if (elementClass == null)
      throw ExceptionFactory.classForElementCannotBeNull();

    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        final String className = getClassName(elementClass);
        getRawGraph().getMetadata().getIndexManager().dropIndex(className + "." + key);
        return null;
      }
    }, "drop key index '", elementClass.getSimpleName(), ".", key, "'");

  }

  /**
   * Creates an automatic indexing structure for indexing provided key for element class.
   *
   * @param key
   *          the key to create the index for
   * @param elementClass
   *          the element class that the index is for
   * @param indexParameters
   *          a collection of parameters for the underlying index implementation:
   *          <ul>
   *          <li>"type" is the index type between the supported types (UNIQUE, NOTUNIQUE, FULLTEXT). The default type is NOT_UNIQUE
   *          <li>"class" is the class to index when it's a custom type derived by Vertex (V) or Edge (E)
   *          <li>"keytype" to use a key type different by OType.STRING,</li>
   *          </li>
   *          </ul>
   * @param <T>
   *          the element class specification
   */
  @SuppressWarnings({ "rawtypes" })
  @Override
  public <T extends Element> void createKeyIndex(final String key, final Class<T> elementClass, final Parameter... indexParameters) {
    makeActive();

    if (elementClass == null)
      throw ExceptionFactory.classForElementCannotBeNull();

    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {

        String indexType = OClass.INDEX_TYPE.NOTUNIQUE.name();
        OType keyType = OType.STRING;
        String className = null;
        String collate = null;
        ODocument metadata = null;

        final String ancestorClassName = getClassName(elementClass);

        // READ PARAMETERS
        for (Parameter<?, ?> p : indexParameters) {
          if (p.getKey().equals("type"))
            indexType = p.getValue().toString().toUpperCase();
          else if (p.getKey().equals("keytype"))
            keyType = OType.valueOf(p.getValue().toString().toUpperCase());
          else if (p.getKey().equals("class"))
            className = p.getValue().toString();
          else if (p.getKey().equals("collate"))
            collate = p.getValue().toString();
          else if (p.getKey().toString().startsWith("metadata.")) {
            if (metadata == null)
              metadata = new ODocument();
            metadata.field(p.getKey().toString().substring("metadata.".length()), p.getValue());
          }
        }

        if (className == null)
          className = ancestorClassName;

        final ODatabaseDocumentTx db = getRawGraph();
        final OSchema schema = db.getMetadata().getSchema();

        final OClass cls = schema.getOrCreateClass(className, schema.getClass(ancestorClassName));
        final OProperty property = cls.getProperty(key);
        if (property != null)
          keyType = property.getType();

        OPropertyIndexDefinition indexDefinition = new OPropertyIndexDefinition(className, key, keyType);
        if (collate != null)
          indexDefinition.setCollate(collate);
        db.getMetadata().getIndexManager()
            .createIndex(className + "." + key, indexType, indexDefinition, cls.getPolymorphicClusterIds(), null, metadata);
        return null;

      }
    }, "create key index on '", elementClass.getSimpleName(), ".", key, "'");
  }

  /**
   * Returns the indexed properties.
   *
   * @param elementClass
   *          the element class that the index is for
   * @return Set of String containing the indexed properties
   */
  @Override
  public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
    makeActive();

    return getIndexedKeys(elementClass, false);
  }

  /**
   * Returns the indexed properties.
   *
   * @param elementClass
   *          the element class that the index is for
   * @param includeClassNames
   *          If true includes also the class name as prefix of fields
   * @return Set of String containing the indexed properties
   */
  public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass, final boolean includeClassNames) {
    makeActive();

    if (elementClass == null)
      throw ExceptionFactory.classForElementCannotBeNull();

    final OSchema schema = getRawGraph().getMetadata().getImmutableSchemaSnapshot();
    final String elementOClassName = getClassName(elementClass);

    Set<String> result = new HashSet<String>();
    final Collection<? extends OIndex<?>> indexes = getRawGraph().getMetadata().getIndexManager().getIndexes();
    for (OIndex<?> index : indexes) {
      String indexName = index.getName();
      int point = indexName.indexOf(".");
      if (point > 0) {
        String oClassName = indexName.substring(0, point);
        OClass oClass = schema.getClass(oClassName);
        if (oClass.isSubClassOf(elementOClassName)) {
          if (includeClassNames)
            result.add(index.getName());
          else
            result.add(index.getDefinition().getFields().get(0));
        }
      }
    }
    return result;
  }

  /**
   * Returns a GraphQuery object to execute queries against the Graph.
   *
   * @return new GraphQuery instance
   */
  @Override
  public GraphQuery query() {
    makeActive();

    return new OrientGraphQuery(this);
  }

  /**
   * Returns a OTraverse object to start traversing the graph.
   */
  public OTraverse traverse() {
    makeActive();

    return new OTraverse();
  }

  /**
   * Executes commands against the graph. Commands are executed outside transaction.
   *
   * @param iCommand
   *          Command request between SQL, GREMLIN and SCRIPT commands
   */
  public OCommandRequest command(final OCommandRequest iCommand) {
    makeActive();

    return new OrientGraphCommand(this, getRawGraph().command(iCommand));
  }

  /**
   * Counts the vertices in graph.
   *
   * @return Long as number of total vertices
   */
  public long countVertices() {
    return countVertices(OrientVertexType.CLASS_NAME);
  }

  /**
   * Counts the vertices in graph of a particular class.
   *
   * @return Long as number of total vertices
   */
  public long countVertices(final String iClassName) {
    makeActive();

    return getRawGraph().countClass(iClassName);
  }

  /**
   * Counts the edges in graph. Edge counting works only if useLightweightEdges is false.
   *
   * @return Long as number of total edges
   */
  public long countEdges() {
    return countEdges(OrientEdgeType.CLASS_NAME);
  }

  /**
   * Counts the edges in graph of a particular class. Edge counting works only if useLightweightEdges is false.
   *
   * @return Long as number of total edges
   */
  public long countEdges(final String iClassName) {
    makeActive();

    if (isUseLightweightEdges())
      throw new UnsupportedOperationException("Graph set to use Lightweight Edges, count against edges is not supported");

    return getRawGraph().countClass(iClassName);
  }

  public <RET> RET executeOutsideTx(final OCallable<RET, OrientBaseGraph> iCallable, final String... iOperationStrings)
      throws RuntimeException {
    makeActive();

    final boolean committed;
    final ODatabaseDocumentTx raw = getRawGraph();
    if (raw.getTransaction().isActive()) {
      if (isWarnOnForceClosingTx() && OLogManager.instance().isWarnEnabled() && iOperationStrings.length > 0) {
        // COMPOSE THE MESSAGE
        final StringBuilder msg = new StringBuilder(256);
        for (String s : iOperationStrings)
          msg.append(s);

        // ASSURE PENDING TX IF ANY IS COMMITTED
        OLogManager
            .instance()
            .warn(
                this,
                "Requested command '%s' must be executed outside active transaction: the transaction will be committed and reopen right after it. To avoid this behavior execute it outside a transaction",
                msg.toString());
      }
      raw.commit();
      committed = true;
    } else
      committed = false;

    try {
      return iCallable.call(this);
    } finally {
      if (committed)
        // RESTART TRANSACTION
        ((OrientTransactionalGraph) this).begin();
    }
  }

  protected void autoStartTransaction() {
  }

  protected void saveIndexConfiguration() {
    getRawGraph().getMetadata().getIndexManager().getConfiguration().save();
  }

  protected <T> String getClassName(final Class<T> elementClass) {
    if (elementClass.isAssignableFrom(Vertex.class))
      return OrientVertexType.CLASS_NAME;
    else if (elementClass.isAssignableFrom(Edge.class))
      return OrientEdgeType.CLASS_NAME;
    throw new IllegalArgumentException("Class '" + elementClass + "' is neither a Vertex, nor an Edge");
  }

  protected Object convertKey(final OIndex<?> idx, Object iValue) {
    if (iValue != null) {
      final OType[] types = idx.getKeyTypes();
      if (types.length == 0)
        iValue = iValue.toString();
      else
        iValue = OType.convert(iValue, types[0].getDefaultJavaType());
    }
    return iValue;
  }

  protected Object[] convertKeys(final OIndex<?> idx, Object[] iValue) {
    if (iValue != null) {

      final OType[] types = idx.getKeyTypes();
      if (types.length == iValue.length) {
        Object[] newValue = new Object[types.length];
        for (int i = 0; i < types.length; i++) {
          newValue[i] = OType.convert(iValue[i], types[i].getDefaultJavaType());
        }
        iValue = newValue;
      }
    }
    return iValue;
  }

  protected void setCurrentGraphInThreadLocal() {
    if (getThreadMode() == THREAD_MODE.MANUAL)
      return;

    final ODatabaseDocument tlDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (getThreadMode() == THREAD_MODE.ALWAYS_AUTOSET || tlDb == null) {
      if (database != null && tlDb != database)
        // SET IT
        ODatabaseRecordThreadLocal.INSTANCE.set(database);
    }
  }

  private void putInInitializationStack() {
    Deque<OrientBaseGraph> stack = initializationStack.get();
    stack.push(this);
  }

  private void pollGraphFromStack(boolean updateDb) {
    final Deque<OrientBaseGraph> stack = initializationStack.get();
    stack.poll();

    final OrientBaseGraph prevGraph = stack.peek();

    if (prevGraph != null) {
      activeGraph.set(prevGraph);
      prevGraph.makeActive();
    } else {
      activeGraph.set(null);
      if (updateDb)
        ODatabaseRecordThreadLocal.INSTANCE.set(null);
    }

  }

  @SuppressWarnings("unchecked")
  private void readDatabaseConfiguration() {
    final ODatabaseDocumentTx databaseDocumentTx = getRawGraph();

    final List<OStorageEntryConfiguration> custom = (List<OStorageEntryConfiguration>) databaseDocumentTx.get(ATTRIBUTES.CUSTOM);
    for (OStorageEntryConfiguration c : custom) {
      if (c.name.equals("useLightweightEdges"))
        setUseLightweightEdges(Boolean.parseBoolean(c.value));
      else if (c.name.equals("useClassForEdgeLabel"))
        setUseClassForEdgeLabel(Boolean.parseBoolean(c.value));
      else if (c.name.equals("useClassForVertexLabel"))
        setUseClassForVertexLabel(Boolean.parseBoolean(c.value));
      else if (c.name.equals("useVertexFieldsForEdgeLabels"))
        setUseVertexFieldsForEdgeLabels(Boolean.parseBoolean(c.value));
      else if (c.name.equals("standardElementConstraints"))
        setStandardElementConstraints(Boolean.parseBoolean(c.value));
    }

  }

  private void openOrCreate() {
    if (url == null)
      throw new IllegalStateException("Database is closed");

    if (pool == null) {
      database = new ODatabaseDocumentTx(url);
      if (url.startsWith("remote:") || database.exists()) {
        if (database.isClosed())
          database.open(username, password);

        // LOAD THE INDEX CONFIGURATION FROM INTO THE DICTIONARY
        // final ODocument indexConfiguration =
        // database.getMetadata().getIndexManager().getConfiguration();
      } else
        database.create();
    } else
      database = pool.acquire();

    makeActive();
    putInInitializationStack();
    checkForGraphSchema(database);
  }

  private List<Index<? extends Element>> loadManualIndexes() {
    final List<Index<? extends Element>> result = new ArrayList<Index<? extends Element>>();
    for (OIndex<?> idx : database.getMetadata().getIndexManager().getIndexes()) {
      if (hasIndexClass(idx))
        // LOAD THE INDEXES
        result.add(new OrientIndex<OrientElement>(this, idx));
    }

    return result;
  }

  private boolean hasIndexClass(OIndex<?> idx) {
    final ODocument metadata = idx.getMetadata();

    return (metadata != null && metadata.field(OrientIndex.CONFIG_CLASSNAME) != null)
    // compatibility with versions earlier 1.6.3
        || idx.getConfiguration().field(OrientIndex.CONFIG_CLASSNAME) != null;
  }

  private static class InitializationStackThreadLocal extends ThreadLocal<Deque<OrientBaseGraph>> {
    @Override
    protected Deque<OrientBaseGraph> initialValue() {
      return new LinkedList<OrientBaseGraph>();
    }
  }
}
