package com.tinkerpop.blueprints.impls.orient;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.configuration.Configuration;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.traverse.OTraverse;
import com.orientechnologies.orient.core.config.OStorageEntryConfiguration;
import com.orientechnologies.orient.core.db.ODatabase.ATTRIBUTES;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.ODatabaseRecord;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
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

/**
 * A Blueprints implementation of the graph database OrientDB (http://www.orientechnologies.com)
 * 
 * @author Luca Garulli (http://www.orientechnologies.com)
 */
public abstract class OrientBaseGraph implements IndexableGraph, MetaGraph<ODatabaseDocumentTx>, KeyIndexableGraph {
  public static final String CONNECTION_OUT = "out";
  public static final String CONNECTION_IN  = "in";
  public static final String CLASS_PREFIX   = "class:";
  public static final String CLUSTER_PREFIX = "cluster:";

  public enum THREAD_MODE {
    MANUAL, AUTOSET_IFNULL, ALWAYS_AUTOSET
  };

  protected final static String                        ADMIN                        = "admin";
  protected boolean                                    useLightweightEdges          = true;
  protected boolean                                    useClassForEdgeLabel         = true;
  protected boolean                                    useClassForVertexLabel       = true;
  protected boolean                                    keepInMemoryReferences       = false;
  protected boolean                                    useVertexFieldsForEdgeLabels = true;
  protected boolean                                    saveOriginalIds              = false;
  protected boolean                                    standardElementConstraints   = true;
  protected boolean                                    warnOnForceClosingTx         = true;
  protected THREAD_MODE                                threadMode                   = THREAD_MODE.AUTOSET_IFNULL;

  private String                                       url;
  private String                                       username;
  private String                                       password;

  private static final ThreadLocal<OrientGraphContext> threadContext                = new ThreadLocal<OrientGraphContext>();
  private static final List<OrientGraphContext>        contexts                     = new ArrayList<OrientGraphContext>();

  /**
   * Constructs a new object using an existent OGraphDatabase instance.
   * 
   * @param iDatabase
   *          Underlying OGraphDatabase object to attach
   */
  public OrientBaseGraph(final ODatabaseDocumentTx iDatabase) {
    reuse(iDatabase);
    readDatabaseConfiguration();
  }

  public OrientBaseGraph(final String url) {
    this(url, ADMIN, ADMIN);
  }

  public OrientBaseGraph(final String url, final String username, final String password) {
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
  }

  /**
   * Drops the database
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
        synchronized (contexts) {
          if (context.manualIndices.containsKey(indexName))
            throw ExceptionFactory.indexAlreadyExists(indexName);

          final OrientIndex<? extends OrientElement> index = new OrientIndex<OrientElement>(g, indexName, indexClass, null);

          // ADD THE INDEX IN ALL CURRENT CONTEXTS
          for (OrientGraphContext ctx : contexts)
            ctx.manualIndices.put(index.getIndexName(), index);
          context.manualIndices.put(index.getIndexName(), index);

          // SAVE THE CONFIGURATION INTO THE GLOBAL CONFIG
          saveIndexConfiguration();

          return (Index<T>) index;
        }
      };
    }, "create index '", indexName, "'");
  }

  @SuppressWarnings("unchecked")
  public <T extends Element> Index<T> getIndex(final String indexName, final Class<T> indexClass) {
    final OrientGraphContext context = getContext(true);
    Index<? extends Element> index = context.manualIndices.get(indexName);
    if (null == index) {
      return null;
    }

    if (indexClass.isAssignableFrom(index.getIndexClass()))
      return (Index<T>) index;
    else
      throw ExceptionFactory.indexDoesNotSupportClass(indexName, indexClass);
  }

  public Iterable<Index<? extends Element>> getIndices() {
    final OrientGraphContext context = getContext(true);
    final List<Index<? extends Element>> list = new ArrayList<Index<? extends Element>>();
    for (Index<?> index : context.manualIndices.values()) {
      list.add(index);
    }
    return list;
  }

  protected Iterable<OrientIndex<? extends OrientElement>> getManualIndices() {
    return getContext(true).manualIndices.values();
  }

  public void dropIndex(final String indexName) {
    executeOutsideTx(new OCallable<Object, OrientBaseGraph>() {
      @Override
      public Object call(OrientBaseGraph g) {
        try {
          String recordMapIndexName = null;
          synchronized (contexts) {
            for (OrientGraphContext ctx : contexts) {
              OrientIndex<?> index = ctx.manualIndices.remove(indexName);
              if (recordMapIndexName == null && index != null)
                recordMapIndexName = index.getUnderlying().getConfiguration().field(OrientIndex.CONFIG_RECORD_MAP_NAME);
            }
          }

          getRawGraph().getMetadata().getIndexManager().dropIndex(indexName);
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

  public OrientVertex addVertex(final Object id) {
    return addVertex(id, (Object[]) null);
  }

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

      if (saveOriginalIds)
        // SAVE THE ID TOO
        fields = new Object[] { OrientElement.DEF_ORIGINAL_ID_FIELDNAME, id };
    }

    this.autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, className, fields);
    vertex.setProperties(prop);

    // SAVE IT
    if (clusterName != null)
      vertex.save(clusterName);
    else
      vertex.save();
    return vertex;
  }

  public OrientVertex addVertex(final String iClassName, final String iClusterName) {
    this.autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, iClassName);

    // SAVE IT
    if (iClusterName != null)
      vertex.save(iClusterName);
    else
      vertex.save();
    return vertex;
  }

  /**
   * Creates a temporary vertex. The vertex is not saved and the transaction is not started.
   * 
   * @param iClassName
   *          Vertex's class name
   * @param prop
   *          Varargs of properties to set
   * @return
   */
  public OrientVertex addTemporaryVertex(final String iClassName, final Object... prop) {
    this.autoStartTransaction();

    final OrientVertex vertex = new OrientVertex(this, iClassName);
    vertex.setProperties(prop);
    return vertex;
  }

  public OrientEdge addEdge(final Object id, final Vertex outVertex, final Vertex inVertex, final String label) {
    String className = null;
    if (id != null && id instanceof String && id.toString().startsWith(CLASS_PREFIX))
      // GET THE CLASS NAME
      className = id.toString().substring(CLASS_PREFIX.length());

    // SAVE THE ID TOO?
    final Object[] fields = saveOriginalIds && id != null ? new Object[] { OrientElement.DEF_ORIGINAL_ID_FIELDNAME, id } : null;

    return ((OrientVertex) outVertex).addEdge(label, (OrientVertex) inVertex, className, null, fields);
  }

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

  public void removeVertex(final Vertex vertex) {
    vertex.remove();
  }

  public Iterable<Vertex> getVertices() {
    return getVerticesOfClass(OrientVertex.CLASS_NAME, true);
  }

  public Iterable<Vertex> getVertices(final boolean iPolymorphic) {
    return getVerticesOfClass(OrientVertex.CLASS_NAME, iPolymorphic);
  }

  public Iterable<Vertex> getVerticesOfClass(final String iClassName) {
    return getVerticesOfClass(iClassName, true);
  }

  public Iterable<Vertex> getVerticesOfClass(final String iClassName, final boolean iPolymorphic) {
    getContext(true);
    return new OrientElementScanIterable<Vertex>(this, iClassName, iPolymorphic);
  }

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
      indexName = OrientVertex.CLASS_NAME + "." + iKey;
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

  public Iterable<Edge> getEdges() {
    return getEdgesOfClass(OrientEdge.CLASS_NAME, true);
  }

  public Iterable<Edge> getEdges(final boolean iPolymorphic) {
    return getEdgesOfClass(OrientEdge.CLASS_NAME, iPolymorphic);
  }

  public Iterable<Edge> getEdgesOfClass(final String iClassName) {
    return getEdgesOfClass(iClassName, true);
  }

  public Iterable<Edge> getEdgesOfClass(final String iClassName, final boolean iPolymorphic) {
    getContext(true);
    return new OrientElementScanIterable<Edge>(this, iClassName, iPolymorphic);
  }

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
      indexName = OrientEdge.CLASS_NAME + "." + iKey;
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

  public void removeEdge(final Edge edge) {
    edge.remove();
  }

  /**
   * Reuses the underlying database avoiding to create and open it every time.
   * 
   * @param iDatabase
   *          Underlying OGraphDatabase object
   */
  public OrientBaseGraph reuse(final ODatabaseDocumentTx iDatabase) {
    this.url = iDatabase.getURL();
    this.username = iDatabase.getUser() != null ? iDatabase.getUser().getName() : null;
    synchronized (this) {
      OrientGraphContext context = threadContext.get();
      if (context == null || !context.rawGraph.getName().equals(iDatabase.getName())) {
        removeContext();
        context = new OrientGraphContext();
        context.rawGraph = iDatabase;
        checkForGraphSchema(iDatabase);
        threadContext.set(context);
      }
    }
    return this;
  }

  protected void checkForGraphSchema(final ODatabaseDocumentTx iDatabase) {
    final OSchema schema = iDatabase.getMetadata().getSchema();

    schema.getOrCreateClass(OMVRBTreeRIDProvider.PERSISTENT_CLASS_NAME);

    final OClass vertexBaseClass = schema.getClass(OrientVertex.CLASS_NAME);
    final OClass edgeBaseClass = schema.getClass(OrientEdge.CLASS_NAME);

    if (vertexBaseClass == null)
      // CREATE THE META MODEL USING THE ORIENT SCHEMA
      schema.createClass(OrientVertex.CLASS_NAME).setOverSize(2);

    if (edgeBaseClass == null)
      schema.createClass(OrientEdge.CLASS_NAME);

    // @COMPATIBILITY < 1.4.0:
    boolean warn = false;
    final String MSG_SUFFIX = ". Probably you are using a database created with a previous version of OrientDB. Export in graphml format and reimport it";

    if (vertexBaseClass != null) {
      if (!vertexBaseClass.getName().equals(OrientVertex.CLASS_NAME)) {
        OLogManager.instance().warn(this, "Found Vertex class %s" + MSG_SUFFIX, vertexBaseClass.getName());
        warn = true;
      }

      if (vertexBaseClass.existsProperty(CONNECTION_OUT) || vertexBaseClass.existsProperty(CONNECTION_IN)) {
        OLogManager.instance().warn(this, "Found property in/out against V");
        warn = true;
      }
    }

    if (edgeBaseClass != null) {
      if (!warn && !edgeBaseClass.getName().equals(OrientEdge.CLASS_NAME)) {
        OLogManager.instance().warn(this, "Found Edge class %s" + MSG_SUFFIX, edgeBaseClass.getName());
        warn = true;
      }

      if (edgeBaseClass.existsProperty(CONNECTION_OUT) || edgeBaseClass.existsProperty(CONNECTION_IN)) {
        OLogManager.instance().warn(this, "Found property in/out against E");
        warn = true;
      }
    }
  }

  public boolean isClosed() {
    final OrientGraphContext context = getContext(false);
    if (context == null)
      return true;

    return context.rawGraph.isClosed();
  }

  public void shutdown() {
    removeContext();

    url = null;
    username = null;
    password = null;
  }

  public String toString() {
    return StringFactory.graphString(this, getRawGraph().getURL());
  }

  public ODatabaseDocumentTx getRawGraph() {
    return getContext(true).rawGraph;
  }

  public void commit() {
  }

  public void rollback() {
  }

  public OClass getVertexBaseType() {
    return getRawGraph().getMetadata().getSchema().getClass(OrientVertex.CLASS_NAME);
  }

  public final OClass getVertexType(final String iTypeName) {
    final OClass cls = getRawGraph().getMetadata().getSchema().getClass(iTypeName);
    if (cls != null)
      checkVertexType(cls);
    return cls;
  }

  public OClass createVertexType(final String iClassName) {
    return createVertexType(iClassName, (String) null);
  }

  public OClass createVertexType(final String iClassName, final String iSuperClassName) {
    return createVertexType(iClassName, iSuperClassName == null ? getVertexBaseType() : getVertexType(iSuperClassName));
  }

  public OClass createVertexType(final String iClassName, final OClass iSuperClass) {
    checkVertexType(iSuperClass);

    return executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        return getRawGraph().getMetadata().getSchema().createClass(iClassName, iSuperClass);
      }
    }, "create vertex type '", iClassName, "' as subclass of '", iSuperClass.getName(), "'");
  }

  public final void dropVertexType(final String iTypeName) {
    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        getRawGraph().getMetadata().getSchema().dropClass(iTypeName);
        return null;
      }
    }, "drop vertex type '", iTypeName, "'");
  }

  public OClass getEdgeBaseType() {
    return getRawGraph().getMetadata().getSchema().getClass(OrientEdge.CLASS_NAME);
  }

  public final OClass getEdgeType(final String iTypeName) {
    final OClass cls = getRawGraph().getMetadata().getSchema().getClass(iTypeName);
    if (cls != null)
      checkEdgeType(cls);
    return cls;
  }

  public OClass createEdgeType(final String iClassName) {
    return createEdgeType(iClassName, (String) null);
  }

  public OClass createEdgeType(final String iClassName, final String iSuperClassName) {
    return createEdgeType(iClassName, iSuperClassName == null ? getEdgeBaseType() : getEdgeType(iSuperClassName));
  }

  public OClass createEdgeType(final String iClassName, final OClass iSuperClass) {
    checkEdgeType(iSuperClass);
    return executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        return getRawGraph().getMetadata().getSchema().createClass(iClassName, iSuperClass);
      }
    }, "create edge type '", iClassName, "' as subclass of '", iSuperClass.getName(), "'");
  }

  public final void dropEdgeType(final String iTypeName) {
    executeOutsideTx(new OCallable<OClass, OrientBaseGraph>() {
      @Override
      public OClass call(final OrientBaseGraph g) {
        getRawGraph().getMetadata().getSchema().dropClass(iTypeName);
        return null;
      }
    }, "drop edge type '", iTypeName, "'");
  }

  protected final void checkVertexType(final OClass iType) {
    if (iType == null)
      throw new IllegalArgumentException("Vertex class is null");

    if (!iType.isSubClassOf(OrientVertex.CLASS_NAME))
      throw new IllegalArgumentException("Type error. The class " + iType + " does not extend class '" + OrientVertex.CLASS_NAME
          + "' and therefore cannot be considered a Vertex");
  }

  protected final void checkEdgeType(final OClass iType) {
    if (iType == null)
      throw new IllegalArgumentException("Edge class is null");

    if (!iType.isSubClassOf(OrientEdge.CLASS_NAME))
      throw new IllegalArgumentException("Type error. The class " + iType + " does not extend class '" + OrientEdge.CLASS_NAME
          + "' and therefore cannot be considered an Edge");
  }

  /**
   * Returns a graph element, vertex or edge, starting from an ID.
   * 
   * @param id
   *          element id
   * @return OrientElement subclass such as OrientVertex or OrientEdge
   */
  public OrientElement getElement(final Object id) {
    if (null == id)
      throw ExceptionFactory.vertexIdCanNotBeNull();

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
      if (schemaClass.isSubClassOf(OrientVertex.CLASS_NAME)) {
        return new OrientVertex(this, doc);
      } else if (schemaClass.isSubClassOf(OrientEdge.CLASS_NAME)) {
        return new OrientEdge(this, doc);
      } else
        throw new IllegalArgumentException("Type error. The class " + schemaClass + " does not extend class neither '"
            + OrientVertex.CLASS_NAME + "' nor '" + OrientEdge.CLASS_NAME + "'");
    }

    return null;
  }

  protected void autoStartTransaction() {
    setCurrentGraphInThreadLocal();
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

      context.rawGraph = new ODatabaseDocumentTx(url);

      if (url.startsWith("remote:") || context.rawGraph.exists()) {
        context.rawGraph.open(username, password);

        // LOAD THE INDEX CONFIGURATION FROM INTO THE DICTIONARY
        // final ODocument indexConfiguration =
        // context.rawGraph.getMetadata().getIndexManager().getConfiguration();

        for (OIndex<?> idx : context.rawGraph.getMetadata().getIndexManager().getIndexes()) {
          ODocument metadata = idx.getMetadata();
          if (metadata != null && metadata.field(OrientIndex.CONFIG_CLASSNAME) != null)
            // LOAD THE INDEXES
            loadIndex(idx);
        }

      } else
        context.rawGraph.create();

      checkForGraphSchema(context.rawGraph);

      return context;
    }
  }

  @SuppressWarnings({ "unchecked", "rawtypes" })
  private OrientIndex<?> loadIndex(final OIndex<?> rawIndex) {
    final OrientIndex<?> index = new OrientIndex(this, rawIndex);

    // REGISTER THE INDEX
    getContext(true).manualIndices.put(index.getIndexName(), index);
    return index;
  }

  private void removeContext() {
    final OrientGraphContext context = getContext(false);

    if (context != null) {
      context.manualIndices.clear();

      if (!context.rawGraph.isClosed()) {
        context.rawGraph.commit();
        context.rawGraph.close();
      }

      synchronized (contexts) {
        contexts.remove(context);
      }

      threadContext.set(null);
    }
  }

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
   * Create an automatic indexing structure for indexing provided key for element class.
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

  public <T extends Element> Set<String> getIndexedKeys(final Class<T> elementClass) {
    return getIndexedKeys(elementClass, false);
  }

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

  public boolean isUseLightweightEdges() {
    return useLightweightEdges;
  }

  public void setUseLightweightEdges(final boolean useDynamicEdges) {
    this.useLightweightEdges = useDynamicEdges;
  }

  public boolean isSaveOriginalIds() {
    return saveOriginalIds;
  }

  public void setSaveOriginalIds(final boolean saveIds) {
    this.saveOriginalIds = saveIds;
  }

  public long countVertices() {
    return getRawGraph().countClass(OrientVertex.CLASS_NAME);
  }

  public long countVertices(final String iClassName) {
    return getRawGraph().countClass(iClassName);
  }

  public long countEdges() {
    if (useLightweightEdges)
      throw new UnsupportedOperationException("Graph set to use Lightweight Edges, count against edges is not supported");

    return getRawGraph().countClass(OrientEdge.CLASS_NAME);
  }

  public long countEdges(final String iClassName) {
    if (useLightweightEdges)
      throw new UnsupportedOperationException("Graph set to use Lightweight Edges, count against edges is not supported");

    return getRawGraph().countClass(iClassName);
  }

  public boolean isKeepInMemoryReferences() {
    return keepInMemoryReferences;
  }

  public void setKeepInMemoryReferences(boolean useReferences) {
    this.keepInMemoryReferences = useReferences;
  }

  public boolean isUseClassForEdgeLabel() {
    return useClassForEdgeLabel;
  }

  public void setUseClassForEdgeLabel(final boolean useCustomClassesForEdges) {
    this.useClassForEdgeLabel = useCustomClassesForEdges;
  }

  public boolean isUseClassForVertexLabel() {
    return useClassForVertexLabel;
  }

  public void setUseClassForVertexLabel(final boolean useCustomClassesForVertex) {
    this.useClassForVertexLabel = useCustomClassesForVertex;
  }

  public boolean isUseVertexFieldsForEdgeLabels() {
    return useVertexFieldsForEdgeLabels;
  }

  public void setUseVertexFieldsForEdgeLabels(final boolean useVertexFieldsForEdgeLabels) {
    this.useVertexFieldsForEdgeLabels = useVertexFieldsForEdgeLabels;
  }

  public boolean isStandardElementConstraints() {
    return standardElementConstraints;
  }

  public void setStandardElementConstraints(final boolean allowsPropertyValueNull) {
    this.standardElementConstraints = allowsPropertyValueNull;
  }

  public static void encodeClassNames(final String... iLabels) {
    if (iLabels != null)
      // ENCODE LABELS
      for (int i = 0; i < iLabels.length; ++i)
        iLabels[i] = encodeClassName(iLabels[i]);
  }

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

  protected <T> String getClassName(final Class<T> elementClass) {
    if (elementClass.isAssignableFrom(Vertex.class))
      return OrientVertex.CLASS_NAME;
    else if (elementClass.isAssignableFrom(Edge.class))
      return OrientEdge.CLASS_NAME;
    throw new IllegalArgumentException("Class '" + elementClass + "' is neither a Vertex, nor an Edge");
  }

  protected <RET> RET executeOutsideTx(final OCallable<RET, OrientBaseGraph> iCallable, final String... iOperationStrings) {
    final boolean committed;
    final ODatabaseDocumentTx raw = getRawGraph();
    if (raw.getTransaction().isActive()) {
      if (warnOnForceClosingTx && OLogManager.instance().isWarnEnabled()) {
        // COMPOSE THE MESSAGE
        final StringBuilder msg = new StringBuilder();
        for (String s : iOperationStrings)
          msg.append(s);

        // ASSURE PENDING TX IF ANY IS COMMITTED
        OLogManager.instance().warn(this,
            "Committing the active transaction to %s. To avoid this behavior do it outside the transaction", msg.toString());
      }
      raw.commit();
      committed = true;
    } else
      committed = false;

    try {
      return iCallable.call(this);
    } catch (Exception e) {
      if (e instanceof RuntimeException)
        throw (RuntimeException) e;
      else
        throw new OException(e);
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
    return threadMode;
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
    this.threadMode = iControl;
    return this;
  }

  protected void setCurrentGraphInThreadLocal() {
    if (threadMode == THREAD_MODE.MANUAL)
      return;

    final ODatabaseRecord tlDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
    if (threadMode == THREAD_MODE.ALWAYS_AUTOSET || tlDb == null) {
      final OrientGraphContext ctx = getContext(true);

      if (ctx != null && (tlDb == null || tlDb != ctx.rawGraph))
        // SET IT
        ODatabaseRecordThreadLocal.INSTANCE.set(getRawGraph());
    }
  }

  public boolean isWarnOnForceClosingTx() {
    return warnOnForceClosingTx;
  }

  public OrientBaseGraph setWarnOnForceClosingTx(final boolean warnOnSchemaChangeInTx) {
    this.warnOnForceClosingTx = warnOnSchemaChangeInTx;
    return this;
  }
}