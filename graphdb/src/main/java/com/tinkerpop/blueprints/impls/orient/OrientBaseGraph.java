package com.tinkerpop.blueprints.impls.orient;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentPool;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OCompositeKey;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.type.tree.provider.OMVRBTreeRIDProvider;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.Element;
import com.tinkerpop.blueprints.GraphQuery;
import com.tinkerpop.blueprints.Index;
import com.tinkerpop.blueprints.IndexableGraph;
import com.tinkerpop.blueprints.KeyIndexableGraph;
import com.tinkerpop.blueprints.MetaGraph;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientBaseGraph implements IndexableGraph, MetaGraph<ODatabaseDocumentTx>, KeyIndexableGraph {
  public static final String                    CONNECTION_OUT  = "out";
  public static final String                    CONNECTION_IN   = "in";
  public static final String                    CLASS_PREFIX    = "class:";
  public static final String                    CLUSTER_PREFIX  = "cluster:";
  protected final static String                 ADMIN           = "admin";
  private static final Object                   manualIndexLock = new Object();
  private final ThreadLocal<OrientGraphContext> threadContext   = new ThreadLocal<OrientGraphContext>();
  private final Set<OrientGraphContext>         contexts        = new HashSet<OrientGraphContext>();
  private final ODatabaseDocumentPool           pool;
  protected Settings                            settings        = new Settings();
  private String                                url;
  private String                                username;
  private String                                password;

  public enum THREAD_MODE {
    MANUAL, AUTOSET_IFNULL, ALWAYS_AUTOSET
  }

  public class Settings {
    protected boolean     useLightweightEdges          = true;
    protected boolean     useClassForEdgeLabel         = true;
    protected boolean     useClassForVertexLabel       = true;
    protected boolean     keepInMemoryReferences       = false;
    protected boolean     useVertexFieldsForEdgeLabels = true;
    protected boolean     saveOriginalIds              = false;
    protected boolean     standardElementConstraints   = true;
    protected boolean     warnOnForceClosingTx         = true;
    protected THREAD_MODE threadMode                   = THREAD_MODE.AUTOSET_IFNULL;

    public Settings copy() {
      final Settings copy = new Settings();
      copy.useLightweightEdges = useLightweightEdges;
      copy.useClassForEdgeLabel = useClassForEdgeLabel;
      copy.useClassForVertexLabel = useClassForVertexLabel;
      copy.keepInMemoryReferences = keepInMemoryReferences;
      copy.useVertexFieldsForEdgeLabels = useVertexFieldsForEdgeLabels;
      copy.saveOriginalIds = saveOriginalIds;
      copy.standardElementConstraints = standardElementConstraints;
      copy.warnOnForceClosingTx = warnOnForceClosingTx;
      copy.threadMode = threadMode;
      return copy;
    }
  }

  /**
   * Constructs a new object using an existent database instance.
   * 
   * @param iDatabase
   *          Underlying database object to attach
   */
  public OrientBaseGraph(final ODatabaseDocumentTx iDatabase, final String iUserName, final String iUserPassword) {
    this.pool = null;
    this.username = iUserName;
    this.password = iUserPassword;

    reuse(iDatabase);
    readDatabaseConfiguration();
  }

  public OrientBaseGraph(final ODatabaseDocumentPool pool) {
    this.pool = pool;

    final ODatabaseDocumentTx db = pool.acquire();
    this.username = db.getUser() != null ? db.getUser().getName() : null;
    reuse(db);

    readDatabaseConfiguration();
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
   * </table>
   * 
   * @param configuration
   *          of graph
   */
  public OrientBaseGraph(final Configuration configuration) {
    this(configuration.getString("blueprints.orientdb.url", null), configuration.getString("blueprints.orientdb.username", null),
        configuration.getString("blueprints.orientdb.password", null));

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
      e.printStackTrace();
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
      e.printStackTrace();
      return iClassName;
    }
  }

  /**
   * (Blueprints Extension) Drops the database
   */
  public void drop() {
    getRawGraph().drop();
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  public <T extends Element> Index<T> createIndex(final String indexName, final Class<T> indexClass,
      final Parameter... indexParameters) {
    final OrientGraphContext context = getContext(true);

    return executeOutsideTx(new OCallable<Index<T>, OrientBaseGraph>() {
      public Index<T> call(final OrientBaseGraph g) {
        synchronized (manualIndexLock) {
          final ODatabaseDocumentTx database = context.rawGraph;
          final OIndexManager indexManager = database.getMetadata().getIndexManager();

          if (indexManager.getIndex(indexName) != null)
            throw ExceptionFactory.indexAlreadyExists(indexName);

          final OrientIndex<? extends OrientElement> index = new OrientIndex<OrientElement>(g, indexName, indexClass, null);

          // SAVE THE CONFIGURATION INTO THE GLOBAL CONFIG
          saveIndexConfiguration();

          return (Index<T>) index;
        }
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
    final OrientGraphContext context = getContext(true);
    final ODatabaseDocumentTx database = context.rawGraph;
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
    final OrientGraphContext context = getContext(true);
    return loadManualIndexes(context);
  }

  /**
   * Drops an index by name.
   * 
   * @param indexName
   *          Index name
   */
  public void dropIndex(final String indexName) {
    executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
      @Override
      public Object call(OrientBaseGraph g) {
        try {
          synchronized (manualIndexLock) {
            final OIndexManager indexManager = getRawGraph().getMetadata().getIndexManager();
            final OIndex index = indexManager.getIndex(indexName);
            final String recordMapIndexName = index.getConfiguration().field(OrientIndex.CONFIG_RECORD_MAP_NAME);

            indexManager.dropIndex(indexName);
            if (recordMapIndexName != null)
              getRawGraph().getMetadata().getIndexManager().dropIndex(recordMapIndexName);

            saveIndexConfiguration();
            return null;
          }

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
   *          Optional, can contains the Edge's class name by prefixing with "class:"
   * @return The new OrientVertex created
   */
  @Override
  public OrientVertex addVertex(final Object id) {
    return addVertex(id, (Object[]) null);
  }

  /**
   * (Blueprints Extension) Creates a new unconnected vertex in the Graph setting the initial field values.
   * 
   * @param id
   *          Optional, can contains the Edge's class name by prefixing with "class:"
   * @param prop
   *          Fields must be a odd pairs of key/value or a single object as Map containing entries as key/value pairs
   * @return The new OrientVertex created
   */
  public OrientVertex addVertex(final Object id, final Object... prop) {
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
        }
      }

      if (settings.saveOriginalIds)
        // SAVE THE ID TOO
        fields = new Object[] { OrientElement.DEF_ORIGINAL_ID_FIELDNAME, id };
    }

    setCurrentGraphInThreadLocal();
    autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, className, fields);
    vertex.setProperties(prop);

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
    setCurrentGraphInThreadLocal();
    autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, iClassName);
    vertex.setProperties(prop);
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

    if (id != null && id instanceof String && id.toString().startsWith(CLASS_PREFIX))
      // GET THE CLASS NAME
      className = id.toString().substring(CLASS_PREFIX.length());

    // SAVE THE ID TOO?
    final Object[] fields = settings.saveOriginalIds && id != null ? new Object[] { OrientElement.DEF_ORIGINAL_ID_FIELDNAME, id }
        : null;

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

    final ODocument doc = rid.getRecord();
    if (doc == null)
      return null;

    return new OrientVertex(this, doc);
  }

  /**
   * Removes a vertex from the Graph. All the edges connected to the Vertex are automatically removed.
   * 
   * @param vertex
   *          Vertex to remove
   */
  public void removeVertex(final Vertex vertex) {
    vertex.remove();
  }

  /**
   * Get all the Vertices in Graph.
   * 
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices() {
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
    getContext(true);
    return new OrientElementScanIterable<Vertex>(this, iClassName, iPolymorphic);
  }

  /**
   * Get all the Vertices in Graph filtering by field name and value. Example:<code>
   *   Iterable<Vertex> resultset = getVertices("name", "Jay");
   * </code>
   * 
   * @param iKey
   *          Field name
   * @param iValue
   *          Field value
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices(final String iKey, Object iValue) {
    if (iKey.equals("@class"))
      return getVerticesOfClass(iValue.toString());

    String indexName;
    final String key;
    int pos = iKey.indexOf('.');
    if (pos > -1) {
      indexName = iKey;

      final String className = iKey.substring(0, pos);
      key = iKey.substring(iKey.indexOf('.') + 1);

      final OClass clazz = getContext(true).rawGraph.getMetadata().getSchema().getClass(className);

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

    final OIndex<?> idx = getContext(true).rawGraph.getMetadata().getIndexManager().getIndex(indexName);
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
   * Get all the Vertices in Graph filtering by field name and value. Example:<code>
   *   Iterable<Vertex> resultset = getVertices("Person",new String[] {"name","surname"},new Object[] { "Sherlock" ,"Holmes"});
   * </code>
   * 
   * @param iKey
   *          Fields name
   * @param iValue
   *          Fields value
   * @return Vertices as Iterable
   */
  public Iterable<Vertex> getVertices(final String label, final String[] iKey, Object[] iValue) {

    final OClass clazz = getContext(true).rawGraph.getMetadata().getSchema().getClass(label);
    Set<OIndex<?>> indexes = clazz.getInvolvedIndexes(Arrays.asList(iKey));
    if (indexes.iterator().hasNext()) {
      final OIndex<?> idx = indexes.iterator().next();
      if (idx != null) {
        List<Object> keys = Arrays.asList(convertKeys(idx, iValue));
        OCompositeKey compositeKey = new OCompositeKey(keys);
        Object indexValue = idx.get(compositeKey);
        if (indexValue != null && !(indexValue instanceof Iterable<?>))
          indexValue = Arrays.asList(indexValue);

        return new OrientElementIterable<Vertex>(this, (Iterable<?>) indexValue);
      }
    }
    // NO INDEX: EXECUTE A QUERY
    GraphQuery query = query();
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
    getContext(true);
    return new OrientElementScanIterable<Edge>(this, iClassName, iPolymorphic);
  }

  /**
   * Get all the Edges in Graph filtering by field name and value. Example:<code>
   *   Iterable<Edges> resultset = getEdges("name", "Jay");
   * </code>
   * 
   * @param iKey
   *          Field name
   * @param iValue
   *          Field value
   * @return Edges as Iterable
   */
  public Iterable<Edge> getEdges(final String iKey, Object iValue) {
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

    final OIndex<?> idx = getContext(true).rawGraph.getMetadata().getIndexManager().getIndex(indexName);
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

    return new OrientEdge(this, rec);
  }

  /**
   * Removes an edge from the Graph.
   * 
   * @param edge
   *          Edge to remove
   */
  public void removeEdge(final Edge edge) {
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
    synchronized (this) {
      OrientGraphContext context = threadContext.get();
      if (context == null || !context.rawGraph.getName().equals(iDatabase.getName()) || context.rawGraph.isClosed()) {
        removeContext();
        context = new OrientGraphContext();
        context.rawGraph = iDatabase;
        checkForGraphSchema(iDatabase);
        threadContext.set(context);
        contexts.add(context);
      }
    }
    return this;
  }

  /**
   * Checks if the Graph has been closed.
   * 
   * @return True if it is closed, otherwise false
   */
  public boolean isClosed() {
    final OrientGraphContext context = getContext(false);
    return context == null || context.rawGraph.isClosed();
  }

  /**
   * Closes the Graph. After closing the Graph cannot be used.
   */
  public void shutdown() {
    removeContext();

    url = null;
    username = null;
    password = null;
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
    return getContext(true).rawGraph;
  }

  /**
   * Commits the current active transaction.
   */
  public void commit() {
  }

  /**
   * Rollbacks the current active transaction. All the pending changes are rollbacked.
   */
  public void rollback() {
  }

  /**
   * Returns the V persistent class as OrientVertexType instance.
   */
  public OrientVertexType getVertexBaseType() {
    return new OrientVertexType(this, getRawGraph().getMetadata().getSchema().getClass(OrientVertexType.CLASS_NAME));
  }

  /**
   * Returns the persistent class for type iTypeName as OrientVertexType instance.
   * 
   * @param iTypeName
   *          Vertex class name
   */
  public final OrientVertexType getVertexType(final String iTypeName) {
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
    return new OrientEdgeType(this);
  }

  /**
   * Returns the persistent class for type iTypeName as OrientEdgeType instance.
   * 
   * @param iTypeName
   *          Edge class name
   */
  public final OrientEdgeType getEdgeType(final String iTypeName) {
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
      final OClass schemaClass = doc.getSchemaClass();
      if (schemaClass != null && schemaClass.isSubClassOf(OrientEdgeType.CLASS_NAME))
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
    if (elementClass == null)
      throw ExceptionFactory.classForElementCannotBeNull();

    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {

        String indexType = OClass.INDEX_TYPE.NOTUNIQUE.name();
        OType keyType = OType.STRING;
        String className = null;

        final String ancestorClassName = getClassName(elementClass);

        // READ PARAMETERS
        for (Parameter<?, ?> p : indexParameters) {
          if (p.getKey().equals("type"))
            indexType = p.getValue().toString().toUpperCase();
          else if (p.getKey().equals("keytype"))
            keyType = OType.valueOf(p.getValue().toString().toUpperCase());
          else if (p.getKey().equals("class"))
            className = p.getValue().toString();
        }

        if (className == null)
          className = ancestorClassName;

        final ODatabaseDocumentTx db = getRawGraph();
        final OSchema schema = db.getMetadata().getSchema();

        final OClass cls = schema.getOrCreateClass(className, schema.getClass(ancestorClassName));
        final OProperty property = cls.getProperty(key);
        if (property != null)
          keyType = property.getType();

        db.getMetadata()
            .getIndexManager()
            .createIndex(className + "." + key, indexType, new OPropertyIndexDefinition(className, key, keyType),
                cls.getPolymorphicClusterIds(), null, null);
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
    if (elementClass == null)
      throw ExceptionFactory.classForElementCannotBeNull();

    final OSchema schema = getRawGraph().getMetadata().getSchema();
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
    return new OrientGraphQuery(this);
  }

  /**
   * Returns a OTraverse object to start traversing the graph.
   */
  public OTraverse traverse() {
    return new OTraverse();
  }

  /**
   * Executes commands against the graph. Commands are executed outside transaction.
   * 
   * @param iCommand
   *          Command request between SQL, GREMLIN and SCRIPT commands
   */
  public OCommandRequest command(final OCommandRequest iCommand) {
    return new OrientGraphCommand(this, getRawGraph().command(iCommand));
  }

  /**
   * Counts the vertices in graph.
   * 
   * @return Long as number of total vertices
   */
  public long countVertices() {
    return getRawGraph().countClass(OrientVertexType.CLASS_NAME);
  }

  /**
   * Counts the vertices in graph of a particular class.
   * 
   * @return Long as number of total vertices
   */
  public long countVertices(final String iClassName) {
    return getRawGraph().countClass(iClassName);
  }

  /**
   * Counts the edges in graph. Edge counting works only if useLightweightEdges is false.
   * 
   * @return Long as number of total edges
   */
  public long countEdges() {
    if (settings.useLightweightEdges)
      throw new UnsupportedOperationException("Graph set to use Lightweight Edges, count against edges is not supported");

    return getRawGraph().countClass(OrientEdgeType.CLASS_NAME);
  }

  /**
   * Counts the edges in graph of a particular class. Edge counting works only if useLightweightEdges is false.
   * 
   * @return Long as number of total edges
   */
  public long countEdges(final String iClassName) {
    if (settings.useLightweightEdges)
      throw new UnsupportedOperationException("Graph set to use Lightweight Edges, count against edges is not supported");

    return getRawGraph().countClass(iClassName);
  }

  /**
   * Returns true if is using lightweight edges, otherwise false.
   */
  public boolean isUseLightweightEdges() {
    return settings.useLightweightEdges;
  }

  /**
   * Changes the setting about usage of lightweight edges.
   */
  public void setUseLightweightEdges(final boolean useDynamicEdges) {
    settings.useLightweightEdges = useDynamicEdges;
  }

  /**
   * Returns true if it saves the original Id, otherwise false.
   */
  public boolean isSaveOriginalIds() {
    return settings.saveOriginalIds;
  }

  /**
   * Changes the setting about usage of lightweight edges.
   */
  public void setSaveOriginalIds(final boolean saveIds) {
    settings.saveOriginalIds = saveIds;
  }

  /**
   * Returns true if the references are kept in memory.
   */
  public boolean isKeepInMemoryReferences() {
    return settings.keepInMemoryReferences;
  }

  /**
   * Changes the setting about using references in memory.
   */
  public void setKeepInMemoryReferences(boolean useReferences) {
    settings.keepInMemoryReferences = useReferences;
  }

  /**
   * Returns true if the class are use for Edge labels.
   */
  public boolean isUseClassForEdgeLabel() {
    return settings.useClassForEdgeLabel;
  }

  /**
   * Changes the setting to use the Edge class for Edge labels.
   */
  public void setUseClassForEdgeLabel(final boolean useCustomClassesForEdges) {
    settings.useClassForEdgeLabel = useCustomClassesForEdges;
  }

  /**
   * Returns true if the class are use for Vertex labels.
   */
  public boolean isUseClassForVertexLabel() {
    return settings.useClassForVertexLabel;
  }

  /**
   * Changes the setting to use the Vertex class for Vertex labels.
   */
  public void setUseClassForVertexLabel(final boolean useCustomClassesForVertex) {
    this.settings.useClassForVertexLabel = useCustomClassesForVertex;
  }

  /**
   * Returns true if the out/in fields in vertex are post-fixed with edge labels. This improves traversal time by partitioning edges
   * on different collections, one per Edge's class.
   */
  public boolean isUseVertexFieldsForEdgeLabels() {
    return settings.useVertexFieldsForEdgeLabels;
  }

  /**
   * Changes the setting to postfix vertices fields with edge labels. This improves traversal time by partitioning edges on
   * different collections, one per Edge's class.
   */
  public void setUseVertexFieldsForEdgeLabels(final boolean useVertexFieldsForEdgeLabels) {
    this.settings.useVertexFieldsForEdgeLabels = useVertexFieldsForEdgeLabels;
  }

  /**
   * Returns true if Blueprints standard constraints are applied to elements.
   */
  public boolean isStandardElementConstraints() {
    return settings.standardElementConstraints;
  }

  /**
   * Changes the setting to apply the Blueprints standard constraints against elements.
   */
  public void setStandardElementConstraints(final boolean allowsPropertyValueNull) {
    this.settings.standardElementConstraints = allowsPropertyValueNull;
  }

  /**
   * Returns true if the warning is generated on force the graph closing.
   */
  public boolean isWarnOnForceClosingTx() {
    return settings.warnOnForceClosingTx;
  }

  /**
   * Changes the setting to generate a warning if the graph closing has been forced.
   */
  public OrientBaseGraph setWarnOnForceClosingTx(final boolean warnOnSchemaChangeInTx) {
    this.settings.warnOnForceClosingTx = warnOnSchemaChangeInTx;
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
    return settings.threadMode;
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
  public OrientBaseGraph setThreadMode(final THREAD_MODE iControl) {
    this.settings.threadMode = iControl;
    return this;
  }

  /**
   * Removes the current context.
   */
  protected void removeContext() {
    final List<OrientGraphContext> contextsToRemove = new ArrayList<OrientGraphContext>();
    synchronized (contexts) {
      for (OrientGraphContext contextItem : contexts) {
        if (!contextItem.thread.isAlive())
          contextsToRemove.add(contextItem);
      }
    }

    final OrientGraphContext context = getContext(false);
    if (context != null)
      contextsToRemove.add(context);

    for (OrientGraphContext contextItem : contextsToRemove) {
      try {
        if (!contextItem.rawGraph.isClosed())
          contextItem.rawGraph.commit();

      } catch (RuntimeException e) {
        OLogManager.instance().error(this, "Error during context close for db " + url, e);
        throw e;
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during context close for db " + url, e);
        throw new OException("Error during context close for db " + url, e);
      } finally {
        try {
          contextItem.rawGraph.close();
        } catch (Exception e) {
          OLogManager.instance().error(this, "Error during context close for db " + url, e);
        }
      }
    }

    synchronized (contexts) {
      for (OrientGraphContext contextItem : contextsToRemove)
        contexts.remove(contextItem);
    }

    threadContext.set(null);
  }

  protected void checkForGraphSchema(final ODatabaseDocumentTx iDatabase) {
    final OSchema schema = iDatabase.getMetadata().getSchema();

    schema.getOrCreateClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);

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
        OLogManager.instance().warn(this, "Found Vertex class %s" + MSG_SUFFIX, vertexBaseClass.getName());
        warn = true;
      }

      if (vertexBaseClass.existsProperty(CONNECTION_OUT) || vertexBaseClass.existsProperty(CONNECTION_IN)) {
        OLogManager.instance().warn(this, "Found property in/out against V");
        warn = true;
      }
    }

    if (edgeBaseClass != null) {
      if (!warn && !edgeBaseClass.getName().equals(OrientEdgeType.CLASS_NAME)) {
        OLogManager.instance().warn(this, "Found Edge class %s" + MSG_SUFFIX, edgeBaseClass.getName());
        warn = true;
      }

      if (edgeBaseClass.existsProperty(CONNECTION_OUT) || edgeBaseClass.existsProperty(CONNECTION_IN)) {
        OLogManager.instance().warn(this, "Found property in/out against E");
        warn = true;
      }
    }
  }

  protected void autoStartTransaction() {
  }

  protected void saveIndexConfiguration() {
    getRawGraph().getMetadata().getIndexManager().getConfiguration().save();
  }

  protected OrientGraphContext getContext(final boolean create) {
    OrientGraphContext context = threadContext.get();
    if (context == null || !context.rawGraph.getURL().equals(url)) {
      if (create)
        context = openOrCreate();
    }
    return context;
  }

  protected <T> String getClassName(final Class<T> elementClass) {
    if (elementClass.isAssignableFrom(Vertex.class))
      return OrientVertexType.CLASS_NAME;
    else if (elementClass.isAssignableFrom(Edge.class))
      return OrientEdgeType.CLASS_NAME;
    throw new IllegalArgumentException("Class '" + elementClass + "' is neither a Vertex, nor an Edge");
  }

  protected <RET> RET executeOutsideTx(final OCallable<RET, OrientBaseGraph> iCallable, final String... iOperationStrings)
      throws RuntimeException {
    final boolean committed;
    final ODatabaseDocumentTx raw = getRawGraph();
    if (raw.getTransaction().isActive()) {
      if (settings.warnOnForceClosingTx && OLogManager.instance().isWarnEnabled()) {
        // COMPOSE THE MESSAGE
        final StringBuilder msg = new StringBuilder();
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
        autoStartTransaction();
    }
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
    if (settings.threadMode == THREAD_MODE.MANUAL)
      return;

    final ODatabaseRecord tlDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (settings.threadMode == THREAD_MODE.ALWAYS_AUTOSET || tlDb == null) {
      final OrientGraphContext ctx = getContext(true);

      if (ctx != null && (tlDb == null || tlDb != ctx.rawGraph))
        // SET IT
        ODatabaseRecordThreadLocal.INSTANCE.set(getRawGraph());
    }
  }

  @SuppressWarnings("unchecked")
  private void readDatabaseConfiguration() {
    final List<OStorageEntryConfiguration> custom = (List<OStorageEntryConfiguration>) getRawGraph().get(ATTRIBUTES.CUSTOM);
    for (OStorageEntryConfiguration c : custom) {
      if (c.name.equals("useLightweightEdges"))
        setUseLightweightEdges(Boolean.parseBoolean(c.value));
      else if (c.name.equals("useClassForEdgeLabel"))
        setUseClassForEdgeLabel(Boolean.parseBoolean(c.value));
      else if (c.name.equals("useClassForVertexLabel"))
        setUseClassForVertexLabel(Boolean.parseBoolean(c.value));
      else if (c.name.equals("useVertexFieldsForEdgeLabels"))
        setUseVertexFieldsForEdgeLabels(Boolean.parseBoolean(c.value));
    }

    loadManualIndexes(threadContext.get());
  }

  private OrientGraphContext openOrCreate() {
    if (url == null)
      throw new IllegalStateException("Database is closed");

    synchronized (this) {
      OrientGraphContext context = threadContext.get();
      if (context != null)
        removeContext();

      context = new OrientGraphContext();
      threadContext.set(context);

      synchronized (contexts) {
        contexts.add(context);
      }

      if (pool == null) {
        context.rawGraph = new ODatabaseDocumentTx(url);
        if (url.startsWith("remote:") || context.rawGraph.exists()) {
          context.rawGraph.open(username, password);

          // LOAD THE INDEX CONFIGURATION FROM INTO THE DICTIONARY
          // final ODocument indexConfiguration =
          // context.rawGraph.getMetadata().getIndexManager().getConfiguration();
        } else
          context.rawGraph.create();
      } else
        context.rawGraph = pool.acquire();

      checkForGraphSchema(context.rawGraph);

      return context;
    }
  }

  private List<Index<? extends Element>> loadManualIndexes(OrientGraphContext context) {
    final List<Index<? extends Element>> result = new ArrayList<Index<? extends Element>>();
    for (OIndex<?> idx : context.rawGraph.getMetadata().getIndexManager().getIndexes()) {
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
}
