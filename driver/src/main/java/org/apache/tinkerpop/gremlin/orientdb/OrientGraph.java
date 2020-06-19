package org.apache.tinkerpop.gremlin.orientdb;

import static org.apache.tinkerpop.gremlin.orientdb.StreamUtils.asStream;

import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.apache.tinkerpop.gremlin.orientdb.io.OrientIoRegistry;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphCountStrategy;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphMatchStepStrategy;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphStepStrategy;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn("org.apache.tinkerpop.gremlin.orientdb.gremlintest.suite.OrientDBDebugSuite")
public final class OrientGraph implements OGraph {
  static {
    TraversalStrategies.GlobalCache.registerStrategies(
        OrientGraph.class,
        TraversalStrategies.GlobalCache.getStrategies(Graph.class)
            .clone()
            .addStrategies(
                OrientGraphStepStrategy.instance(),
                OrientGraphCountStrategy.instance(),
                OrientGraphMatchStepStrategy.instance()));
  }

  private static final Map<String, String> INTERNAL_CLASSES_TO_TINKERPOP_CLASSES;

  static {
    INTERNAL_CLASSES_TO_TINKERPOP_CLASSES = new HashMap<>();
    INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.put(OClass.VERTEX_CLASS_NAME, Vertex.DEFAULT_LABEL);
    INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.put(OClass.EDGE_CLASS_NAME, Edge.DEFAULT_LABEL);
  }

  public static final String CONFIG_URL = "orient-url";
  public static final String CONFIG_DB_NAME = "orient-db-name";
  public static final String CONFIG_DB_TYPE = "orient-db-type";
  public static final String CONFIG_USER = "orient-user";
  public static final String CONFIG_PASS = "orient-pass";
  public static final String CONFIG_CREATE = "orient-create";
  public static final String CONFIG_OPEN = "orient-open";
  public static final String CONFIG_TRANSACTIONAL = "orient-transactional";
  public static final String CONFIG_POOL_SIZE = "orient-max-poolsize";
  public static final String CONFIG_MAX_PARTITION_SIZE = "orient-max-partitionsize";
  public static final String CONFIG_LABEL_AS_CLASSNAME = "orient-label-as-classname";

  protected ODatabaseDocument database;
  protected final Features features;
  protected final Configuration configuration;
  protected final String user;
  protected final String password;
  protected OrientGraphBaseFactory factory;
  protected boolean shouldCloseFactory = false;
  protected OElementFactory elementFactory;
  protected OrientTransaction tx;

  public static OrientGraph open() {
    return open(
        "memory:orientdb-" + ThreadLocalRandom.current().nextInt(Integer.MAX_VALUE),
        "admin",
        "admin");
  }

  public static OrientGraph open(String url) {
    return open(url, "admin", "admin");
  }

  public static OrientGraph open(String url, String user, String password) {
    BaseConfiguration configuration = new BaseConfiguration();
    configuration.setProperty(CONFIG_URL, url);
    configuration.setProperty(CONFIG_USER, user);
    configuration.setProperty(CONFIG_PASS, password);

    return open(configuration);
  }

  public static OrientGraph open(final Configuration config) {
    OrientGraphFactory factory = new OrientGraphFactory(config);
    if (config.containsKey(CONFIG_POOL_SIZE))
      factory.setupPool(
          config.getInt(CONFIG_MAX_PARTITION_SIZE, 64), config.getInt(CONFIG_POOL_SIZE));

    return new OrientGraph(factory, config, true);
  }

  public OrientGraph(
      OrientGraphBaseFactory factory,
      final ODatabaseDocument database,
      final Configuration configuration,
      final String user,
      final String password) {
    this.factory = factory;
    this.user = user;
    this.password = password;
    this.database = database;
    this.configuration = configuration;
    if (configuration.getBoolean(CONFIG_TRANSACTIONAL, false)) {
      this.features = ODBFeatures.OrientFeatures.INSTANCE_TX;
      this.tx = new OrientTransaction(this);
    } else {
      this.features = ODBFeatures.OrientFeatures.INSTANCE_NOTX;
      this.tx = new OrientNoTransaction(this);
    }

    this.elementFactory = new OElementFactory(this);
  }

  public OrientGraph(final OrientGraphBaseFactory factory, final Configuration configuration) {
    this(factory, configuration, false);
  }

  public OrientGraph(
      final OrientGraphBaseFactory factory,
      final Configuration configuration,
      boolean closeFactory) {
    this.factory = factory;
    this.database = factory.getDatabase(true, true);
    this.user = "";
    this.password = "";
    makeActive();
    this.configuration = configuration;
    if (configuration.getBoolean(CONFIG_TRANSACTIONAL, false)) {
      this.features = ODBFeatures.OrientFeatures.INSTANCE_TX;
      this.tx = new OrientTransaction(this);
    } else {
      this.features = ODBFeatures.OrientFeatures.INSTANCE_NOTX;
      this.tx = new OrientNoTransaction(this);
    }
    this.shouldCloseFactory = closeFactory;
    this.elementFactory = new OElementFactory(this);
  }

  public Features features() {
    return features;
  }

  public ODatabaseDocument database() {
    return database;
  }

  private void makeActiveDb() {
    final ODatabaseDocument tlDb = ODatabaseRecordThreadLocal.instance().getIfDefined();
    if (database != null && tlDb != database) {
      database.activateOnCurrentThread();
    }
  }

  public void makeActive() {
    makeActiveDb();
  }

  private <R> R executeOutsideTx(Function<ODatabaseDocument, R> lamba) {

    if (this.tx().isOpen()) {
      ODatabaseDocument oldDb = getRawDatabase();
      ODatabaseDocument newDb = null;
      try {
        newDb = ((ODatabaseDocumentInternal) oldDb).copy();
        newDb.activateOnCurrentThread();
        return lamba.apply(newDb);
      } finally {
        if (newDb != null) {
          newDb.close();
        }
        oldDb.activateOnCurrentThread();
      }
    } else {
      return lamba.apply(getRawDatabase());
    }
  }

  @Override
  public Vertex addVertex(Object... keyValues) {
    this.tx().readWrite();
    makeActive();

    ElementHelper.legalPropertyKeyValueArray(keyValues);
    if (ElementHelper.getIdValue(keyValues).isPresent())
      throw Vertex.Exceptions.userSuppliedIdsNotSupported();

    String label = ElementHelper.getLabelValue(keyValues).orElse(OClass.VERTEX_CLASS_NAME);
    OrientVertex vertex = elementFactory().createVertex(label);
    vertex.property(keyValues);

    vertex.save();
    return vertex;
  }

  public OGremlinResultSet executeSql(String sql, Object... params) {
    this.tx().readWrite();
    makeActive();
    OResultSet resultSet = database.command(sql, params);
    return new OGremlinResultSet(this, resultSet);
  }

  public OGremlinResultSet executeSql(String sql, Map params) {
    makeActive();
    OResultSet resultSet = database.command(sql, params);
    return new OGremlinResultSet(this, resultSet);
  }

  public OGremlinResultSet querySql(String sql, Object... params) {
    this.tx().readWrite();
    makeActive();
    OResultSet resultSet = database.query(sql, params);
    return new OGremlinResultSet(this, resultSet);
  }

  public OGremlinResultSet querySql(String sql, Map params) {
    makeActive();
    OResultSet resultSet = database.query(sql, params);
    return new OGremlinResultSet(this, resultSet);
  }

  public OGremlinResultSet execute(String language, String script, Map params) {

    makeActive();
    OResultSet resultSet = database.execute(language, script, params);
    return new OGremlinResultSet(this, resultSet);
  }

  @Deprecated
  public Object executeCommand(OCommandRequest command) {
    return command.execute();
  }

  @Override
  public <C extends GraphComputer> C compute(Class<C> graphComputerClass)
      throws IllegalArgumentException {
    throw new NotImplementedException();
  }

  @Override
  public GraphComputer compute() throws IllegalArgumentException {
    throw new NotImplementedException();
  }

  @Override
  public OElementFactory elementFactory() {
    return elementFactory;
  }

  @Override
  public Iterator<Vertex> vertices(Object... vertexIds) {
    this.tx().readWrite();
    makeActive();
    return elements(
        OClass.VERTEX_CLASS_NAME,
        r ->
            elementFactory()
                .wrapVertex(
                    getRawDocument(r)
                        .asVertex()
                        .orElseThrow(
                            () ->
                                new IllegalArgumentException(
                                    String.format("Cannot get a Vertex from document %s", r)))),
        vertexIds);
  }

  /** Convert a label to orientdb class name */
  public String labelToClassName(String label, String prefix) {
    if (configuration.getBoolean(CONFIG_LABEL_AS_CLASSNAME, true)) {
      return label;
    }
    return label.equals(prefix) ? prefix : prefix + "_" + label;
  }

  /** Convert a orientdb class name to label */
  public String classNameToLabel(String className) {
    if (INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.containsKey(className)) {
      return INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.get(className);
    }
    if (configuration.getBoolean(CONFIG_LABEL_AS_CLASSNAME, true)) {
      return className;
    }
    return className.substring(2);
  }

  /**
   * Tries to execute a lambda in a transaction, retrying it if an ONeedRetryException is thrown.
   *
   * <p>If the Graph has an active transaction, then the transaction has to be empty (no operations
   * executed yet) and after the execution you will be in a new transaction.
   *
   * @param nRetries the maximum number of retries (> 0)
   * @param function a lambda containing application code to execute in a commit/retry loop
   * @param <T> the return type of the lambda
   * @return The result of the execution of the lambda
   * @throws IllegalStateException if there are operations in the current transaction
   * @throws ONeedRetryException if the maximum number of retries is executed and all failed with an
   *     ONeedRetryException
   * @throws IllegalArgumentException if nRetries is <= 0
   * @throws UnsupportedOperationException if this type of graph does not support automatic
   *     commit/retry or does not support transactions
   */
  public <T> T executeWithRetry(int nRetries, Function<OrientGraph, T> function) {

    if (!this.features.graph().supportsTransactions()) {
      throw Graph.Exceptions.transactionsNotSupported();
    }
    ODatabaseDocument rawDatabase = this.getRawDatabase();
    return rawDatabase.executeWithRetry(nRetries, (db) -> function.apply(this));
  }

  protected Object convertValue(final OIndex idx, Object iValue) {
    if (iValue != null) {
      final OType[] types = idx.getKeyTypes();
      if (types.length == 0) iValue = iValue.toString();
      else iValue = OType.convert(iValue, types[0].getDefaultJavaType());
    }
    return iValue;
  }

  public Stream<OrientVertex> getIndexedVertices(OIndex index, Iterator<Object> valueIter) {
    return getIndexedElements(index, valueIter, OrientVertex::new);
  }

  public Stream<OrientEdge> getIndexedEdges(OIndex index, Iterator<Object> valueIter) {
    return getIndexedElements(index, valueIter, OrientEdge::new);
  }

  private <ElementType extends OrientElement> Stream<ElementType> getIndexedElements(
      OIndex index,
      Iterator<Object> valuesIter,
      BiFunction<OrientGraph, OIdentifiable, ElementType> newElement) {
    makeActive();

    if (index == null) {
      return Collections.<ElementType>emptyList().stream();
    } else {
      if (!valuesIter.hasNext()) {
        return index.getInternal().stream().map(id -> newElement.apply(this, id.second));
      } else {
        Stream<Object> convertedValues =
            StreamUtils.asStream(valuesIter).map(value -> convertValue(index, value));
        Stream<OIdentifiable> ids =
            convertedValues.flatMap(v -> lookupInIndex(index, v)).filter(r -> r != null);
        Stream<ORecord> records = ids.map(id -> id.getRecord());
        return records.map(r -> newElement.apply(this, getRawDocument(r)));
      }
    }
  }

  private Stream<OIdentifiable> lookupInIndex(OIndex index, Object value) {
    Object fromIndex = index.get(value);
    if (fromIndex instanceof Iterable)
      return StreamUtils.asStream(((Iterable<OIdentifiable>) fromIndex).iterator());
    else return Stream.of((OIdentifiable) fromIndex);
  }

  private OIndexManager getIndexManager() {
    return database.getMetadata().getIndexManager();
  }

  private OSchema getSchema() {
    return database.getMetadata().getSchema();
  }

  public Set<String> getIndexedKeys(String className) {
    Iterator<OIndex> indexes = getIndexManager().getClassIndexes(className).iterator();
    HashSet<String> indexedKeys = new HashSet<>();
    indexes.forEachRemaining(
        index -> {
          index.getDefinition().getFields().forEach(indexedKeys::add);
        });
    return indexedKeys;
  }

  public Set<String> getIndexedKeys(final Class<? extends Element> elementClass, String label) {
    if (Vertex.class.isAssignableFrom(elementClass)) {
      return getVertexIndexedKeys(label);
    } else if (Edge.class.isAssignableFrom(elementClass)) {
      return getEdgeIndexedKeys(label);
    } else {
      throw new IllegalArgumentException("Class is not indexable: " + elementClass);
    }
  }

  public Set<String> getIndexedKeys(final Class<? extends Element> elementClass) {
    if (Vertex.class.isAssignableFrom(elementClass)) {
      return getIndexedKeys(OClass.VERTEX_CLASS_NAME);
    } else if (Edge.class.isAssignableFrom(elementClass)) {
      return getIndexedKeys(OClass.EDGE_CLASS_NAME);
    } else {
      throw new IllegalArgumentException("Class is not indexable: " + elementClass);
    }
  }

  public Set<String> getVertexIndexedKeys(final String label) {
    String className = labelToClassName(label, OClass.VERTEX_CLASS_NAME);
    OClass cls = getSchema().getClass(className);
    if (cls != null && cls.isSubClassOf(OClass.VERTEX_CLASS_NAME)) {
      return getIndexedKeys(className);
    }
    return new HashSet<String>();
  }

  public Set<String> getEdgeIndexedKeys(final String label) {
    String className = labelToClassName(label, OClass.EDGE_CLASS_NAME);
    OClass cls = getSchema().getClass(className);
    if (cls != null && cls.isSubClassOf(OClass.EDGE_CLASS_NAME)) {
      return getIndexedKeys(className);
    }
    return new HashSet<String>();
  }

  @Override
  public Iterator<Edge> edges(Object... edgeIds) {
    this.tx().readWrite();
    makeActive();
    return elements(
        OClass.EDGE_CLASS_NAME,
        r ->
            elementFactory()
                .wrapEdge(
                    getRawDocument(r)
                        .asEdge()
                        .orElseThrow(
                            () ->
                                new IllegalArgumentException(
                                    String.format("Cannot get an Edge from document %s", r)))),
        edgeIds);
  }

  protected <A extends Element> Iterator<A> elements(
      String elementClass, Function<ORecord, A> toA, Object... elementIds) {
    boolean polymorphic = true;
    if (elementIds.length == 0) {
      // return all vertices as stream

      Iterator<ODocument> itty = database.browseClass(elementClass, polymorphic).iterator();
      return asStream(itty).map(toA).iterator();
    } else {
      Stream<ORID> ids = Stream.of(elementIds).map(OrientGraph::createRecordId);
      Stream<ORecord> records =
          ids.filter(ORID::isValid).map(id -> getRecord(id)).filter(r -> r != null);
      return records.map(toA).iterator();
    }
  }

  protected ORecord getRecord(ORID id) {
    try {
      return id.getRecord();
    } catch (ORecordNotFoundException e) {
      throw new NoSuchElementException(
          "The "
              + getClass().getSimpleName().toLowerCase()
              + " with id "
              + id
              + " of type "
              + id.getClass().getSimpleName()
              + " does not exist in the graph");
    }
  }

  private ORID checkId(ORID id) {
    if (!id.isValid()) throw new IllegalArgumentException("Invalid id " + id);
    return id;
  }

  protected static ORID createRecordId(Object id) {
    if (id instanceof ORecordId) return (ORecordId) id;
    if (id instanceof String) return new ORecordId((String) id);
    if (id instanceof OrientElement) return ((OrientElement) id).id();

    throw new IllegalArgumentException(
        "Orient IDs have to be a String or ORecordId - you provided a " + id.getClass());
  }

  protected OElement getRawDocument(ORecord record) {
    if (record == null) throw new NoSuchElementException();
    record = record.getRecord();
    ODocument currentDocument = (ODocument) record;
    if (currentDocument.getInternalStatus() == ODocument.STATUS.NOT_LOADED) currentDocument.load();
    if (ODocumentInternal.getImmutableSchemaClass(currentDocument) == null)
      throw new IllegalArgumentException(
          "Cannot determine the graph element type because the document class is null. Probably this is a projection, use the EXPAND() function");
    return currentDocument;
  }

  @Override
  public OrientTransaction tx() {
    makeActive();
    return this.tx;
  }

  /** (Blueprints Extension) Drops the database */
  public void drop() {
    factory.drop();
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

  public void begin() {
    makeActive();
    this.tx().doOpen();
  }

  public void commit() {
    makeActive();
    this.tx().commit();
  }

  public void rollback() {
    makeActive();
    this.tx().rollback();
  }

  @Override
  public Variables variables() {
    makeActive();
    throw new NotImplementedException();
  }

  @Override
  public Configuration configuration() {
    return configuration;
  }

  @Override
  public void close() {
    makeActive();
    String url = database.getURL();
    try {
      this.tx().close();
    } finally {
      try {
        if (!database.isClosed()) {
          database.close();
          if (shouldCloseFactory) {
            factory.close();
          }
        }
      } catch (Exception e) {
        OLogManager.instance().error(this, "Error during context close for db " + url, e);
      }
    }
  }

  public String createVertexClass(final String label) {
    makeActive();
    String className = labelToClassName(label, OClass.VERTEX_CLASS_NAME);
    createClass(className, OClass.VERTEX_CLASS_NAME);
    return className;
  }

  public String createEdgeClass(final String label) {
    makeActive();
    String className = labelToClassName(label, OClass.EDGE_CLASS_NAME);
    createClass(className, OClass.EDGE_CLASS_NAME);
    return className;
  }

  public void createClass(final String className, final String superClassName) {
    makeActive();
    OClass superClass = database.getMetadata().getSchema().getClass(superClassName);
    if (superClass == null) {
      Collection<OClass> allClasses = database.getMetadata().getSchema().getClasses();
      throw new IllegalArgumentException(
          "unable to find class " + superClassName + ". Available classes: " + allClasses);
    }
    createClass(className, superClass);
  }

  @Override
  public boolean existClass(String label) {
    makeActive();
    OSchema schema = database.getMetadata().getSchema();
    OClass cls = schema.getClass(label);
    return cls != null;
  }

  public void createClass(final String className, final OClass superClass) {
    makeActive();
    OSchema schema = database.getMetadata().getSchema();
    OClass cls = schema.getClass(className);
    if (cls == null) {
      try {
        executeOutsideTx(
            (db) -> {
              OSchema s = db.getMetadata().getSchema();
              s.createClass(className, superClass);
              return null;
            });
        if (this.tx().isOpen()) schema.reload();
      } catch (OException e) {
        throw new IllegalArgumentException(e);
      }
      OLogManager.instance()
          .info(this, "created class '" + className + "' as subclass of '" + superClass + "'");
    } else {
      if (!cls.isSubClassOf(superClass)) {
        throw new IllegalArgumentException(
            "unable to create class '"
                + className
                + "' as subclass of '"
                + superClass
                + "'. different super class.");
      }
    }
  }

  public ODatabaseDocument getRawDatabase() {
    makeActive();
    return database;
  }

  protected <E> String getClassName(final Class<T> elementClass) {
    if (elementClass.isAssignableFrom(Vertex.class)) return OClass.VERTEX_CLASS_NAME;
    else if (elementClass.isAssignableFrom(Edge.class)) return OClass.EDGE_CLASS_NAME;
    throw new IllegalArgumentException(
        "Class '" + elementClass + "' is neither a Vertex, nor an Edge");
  }

  protected void prepareIndexConfiguration(Configuration config) {
    String defaultIndexType = OClass.INDEX_TYPE.NOTUNIQUE.name();
    OType defaultKeyType = OType.STRING;
    String defaultClassName = null;
    String defaultCollate = null;
    ODocument defaultMetadata = null;

    if (!config.containsKey("type")) config.setProperty("type", defaultIndexType);
    if (!config.containsKey("keytype")) config.setProperty("keytype", defaultKeyType);
    if (!config.containsKey("class")) config.setProperty("class", defaultClassName);
    if (!config.containsKey("collate")) config.setProperty("collate", defaultCollate);
    if (!config.containsKey("metadata")) config.setProperty("metadata", defaultMetadata);
  }

  public <E extends Element> void createVertexIndex(
      final String key, final String label, final Configuration configuration) {
    String className = labelToClassName(label, OClass.VERTEX_CLASS_NAME);
    createVertexClass(label);
    createIndex(key, className, configuration);
  }

  public <E extends Element> void createEdgeIndex(
      final String key, final String label, final Configuration configuration) {
    String className = labelToClassName(label, OClass.EDGE_CLASS_NAME);
    createEdgeClass(label);
    createIndex(key, className, configuration);
  }

  private <E extends Element> void createIndex(
      final String key, String className, final Configuration configuration) {
    makeActive();

    prepareIndexConfiguration(configuration);

    OCallable<OClass, OrientGraph> callable =
        new OCallable<OClass, OrientGraph>() {
          @Override
          public OClass call(final OrientGraph g) {

            String indexType = configuration.getString("type");
            OType keyType = (OType) configuration.getProperty("keytype");
            String collate = configuration.getString("collate");
            ODocument metadata = (ODocument) configuration.getProperty("metadata");

            final ODatabaseDocument db = getRawDatabase();
            final OSchema schema = db.getMetadata().getSchema();

            final OClass cls = schema.getClass(className);
            final OProperty property = cls.getProperty(key);
            if (property != null) keyType = property.getType();

            OPropertyIndexDefinition indexDefinition =
                new OPropertyIndexDefinition(className, key, keyType);
            if (collate != null) indexDefinition.setCollate(collate);
            db.getMetadata()
                .getIndexManager()
                .createIndex(
                    className + "." + key,
                    indexType,
                    indexDefinition,
                    cls.getPolymorphicClusterIds(),
                    null,
                    metadata);
            return null;
          }
        };
    execute(callable, "create key index on '", className, ".", key, "'");
  }

  public <RET> RET execute(
      final OCallable<RET, OrientGraph> iCallable, final String... iOperationStrings)
      throws RuntimeException {
    makeActive();

    if (OLogManager.instance().isWarnEnabled() && iOperationStrings.length > 0) {
      // COMPOSE THE MESSAGE
      final StringBuilder msg = new StringBuilder(256);
      for (String s : iOperationStrings) msg.append(s);

      // ASSURE PENDING TX IF ANY IS COMMITTED
      OLogManager.instance().warn(this, msg.toString());
    }
    return iCallable.call(this);
  }

  @Override
  public <I extends Io> I io(Io.Builder<I> builder) {
    return (I)
        OGraph.super.io(builder.onMapper(mb -> mb.addRegistry(OrientIoRegistry.getInstance())));
  }

  @Override
  public String toString() {
    return StringFactory.graphString(this, database.getURL());
  }

  protected boolean isTransactionActive() {

    return getRawDatabase().getTransaction().isActive();
  }

  public void setElementFactory(OElementFactory elementFactory) {
    this.elementFactory = elementFactory;
  }
}
