package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OCallable;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.index.OIndexManager;
import com.orientechnologies.orient.core.index.OPropertyIndexDefinition;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.*;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import com.orientechnologies.orient.core.storage.OStorage;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphStepStrategy;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.process.traversal.TraversalStrategies;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.io.Io;
import org.apache.tinkerpop.gremlin.structure.io.Io.Builder;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.*;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.orientdb.StreamUtils.asStream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_PERFORMANCE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_PROCESS_COMPUTER)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_INTEGRATE)
@Graph.OptIn(Graph.OptIn.SUITE_GROOVY_ENVIRONMENT_PERFORMANCE)
public final class OrientGraph implements Graph {
    static {
        TraversalStrategies.GlobalCache.registerStrategies(
                OrientGraph.class,
                TraversalStrategies.GlobalCache.getStrategies(Graph.class).clone()
                        .addStrategies(OrientGraphStepStrategy.instance()));
    }

    private static final Map<String, String> INTERNAL_CLASSES_TO_TINKERPOP_CLASSES;

    static {
        INTERNAL_CLASSES_TO_TINKERPOP_CLASSES = new HashMap<>();
        INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.put(OImmutableClass.VERTEX_CLASS_NAME, Vertex.DEFAULT_LABEL);
        INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.put(OImmutableClass.EDGE_CLASS_NAME, Edge.DEFAULT_LABEL);
    }

    public static String CONFIG_URL = "orient-url";
    public static String CONFIG_USER = "orient-user";
    public static String CONFIG_PASS = "orient-pass";
    public static String CONFIG_CREATE = "orient-create";
    public static String CONFIG_OPEN = "orient-open";
    public static String CONFIG_TRANSACTIONAL = "orient-transactional";
    public static String CONFIG_POOL_SIZE = "orient-max-poolsize";
    public static String CONFIG_MAX_PARTITION_SIZE = "orient-max-partitionsize";
    public static String CONFIG_LABEL_AS_CLASSNAME = "orient-label-as-classname";

    protected boolean connectionFailed;
    protected ODatabaseDocumentTx database;
    protected final Features features;
    protected final Configuration configuration;
    protected final OPartitionedReCreatableDatabasePool pool;
    protected final String user;
    protected final String password;

    public static OrientGraph open(final Configuration config) {
        OrientGraphFactory factory = new OrientGraphFactory(config);
        if (config.containsKey(CONFIG_POOL_SIZE))
            if (config.containsKey(CONFIG_MAX_PARTITION_SIZE))
            factory.setupPool(config.getInt(CONFIG_MAX_PARTITION_SIZE), config.getInt(CONFIG_POOL_SIZE));
            else
                factory.setupPool(config.getInt(CONFIG_POOL_SIZE));

        return factory.getNoTx();
    }

    public OrientGraph(final ODatabaseDocumentTx database, final Configuration configuration, final String user, final String password) {
        this.pool = null;
        this.user = user;
        this.password = password;
        this.database = database;
        this.configuration = configuration;
        this.connectionFailed = false;
        if (configuration.getBoolean(CONFIG_TRANSACTIONAL, false)) {
            this.features = ODBFeatures.OrientFeatures.INSTANCE_TX;
        } else {
            this.features = ODBFeatures.OrientFeatures.INSTANCE_NOTX;
        }
    }

    public OrientGraph(final OPartitionedReCreatableDatabasePool pool, final Configuration configuration) {
        this.pool = pool;
        this.database = pool.acquire();
        this.user = "";
        this.password = "";
        this.connectionFailed = false;
        makeActive();
        this.configuration = configuration;
        if (configuration.getBoolean(CONFIG_TRANSACTIONAL, false)) {
            this.features = ODBFeatures.OrientFeatures.INSTANCE_TX;
        } else {
            this.features = ODBFeatures.OrientFeatures.INSTANCE_NOTX;
        }
    }

    public Features features() {
        return features;
    }

    public ODatabaseDocumentTx database() {
        return database;
    }

    private void makeActiveDb() {
        final ODatabaseDocument tlDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
        if (database != null && tlDb != database) {
            database.activateOnCurrentThread();
            ODatabaseRecordThreadLocal.INSTANCE.set(database);
        }
    }

    public void makeActive() {
        makeActiveDb();

        if (this.connectionFailed) {
            this.connectionFailed = false;

            try {
                if (this.pool != null) {
                    this.pool.reCreatePool();
                    this.database = this.pool.acquire();
                } else {
                    ODatabaseDocumentTx replaceDb = new ODatabaseDocumentTx(this.database.getURL(), this.database.isKeepStorageOpen());
                    replaceDb.open(user, password);
                    this.database = replaceDb;
                }
                makeActiveDb();
            } catch (OException e) {
                OLogManager.instance().info(this, "Recreation of connection resulted in exception", e);
            }
        }
    }

    private <T> T executeWithConnectionCheck(Supplier<T> toExecute) {
        try {
            T result = toExecute.get();
            this.connectionFailed = false;
            return result;
        } catch (OException e) {
            this.connectionFailed = true;
            OLogManager.instance().info(this, "Error during db request", e);
            throw e;
        }
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        return executeWithConnectionCheck(() -> {
            makeActive();

            ElementHelper.legalPropertyKeyValueArray(keyValues);
            if (ElementHelper.getIdValue(keyValues).isPresent()) throw Vertex.Exceptions.userSuppliedIdsNotSupported();

            String label = ElementHelper.getLabelValue(keyValues).orElse(OImmutableClass.VERTEX_CLASS_NAME);
            OrientVertex vertex = new OrientVertex(this, label);
            vertex.property(keyValues);

            vertex.save();
            return vertex;
        });
    }

    public Object executeSql(String sql) {
        return executeWithConnectionCheck(() -> {
            makeActive();
            OCommandRequest command = database.command(new OCommandSQL(sql));
            return command.execute();
        });
    }

    public Object executeCommand(OCommandRequest command) {
        return executeWithConnectionCheck(() -> {
            return command.execute();
        });
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> graphComputerClass) throws IllegalArgumentException {
        throw new NotImplementedException();
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw new NotImplementedException();
    }

    @Override
    public Iterator<Vertex> vertices(Object... vertexIds) {
        return executeWithConnectionCheck(() -> {
            makeActive();
            return elements(
                    OImmutableClass.VERTEX_CLASS_NAME,
                    r -> new OrientVertex(this, getRawDocument(r)),
                    vertexIds);
        });
    }

    /**
     * Convert a label to orientdb class name
     */
    public String labelToClassName(String label, String prefix) {
        if (configuration.getBoolean(CONFIG_LABEL_AS_CLASSNAME, false)) {
            return label;
        }
        return label.equals(prefix) ? prefix : prefix + "_" + label;
    }

    /**
     * Convert a orientdb class name to label
     */
    public String classNameToLabel(String className) {
        if (INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.containsKey(className)) {
            return INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.get(className);
        }
        if (configuration.getBoolean(CONFIG_LABEL_AS_CLASSNAME, false)) {
            return className;
        }
        return className.substring(2);
    }

    protected Object convertValue(final OIndex<?> idx, Object iValue) {
        if (iValue != null) {
            final OType[] types = idx.getKeyTypes();
            if (types.length == 0)
                iValue = iValue.toString();
            else
                iValue = OType.convert(iValue, types[0].getDefaultJavaType());
        }
        return iValue;
    }

    public Stream<OrientVertex> getIndexedVertices(OIndex<Object> index, Iterator<Object> valueIter) {
        return getIndexedElements(index, valueIter, OrientVertex::new);
    }

    public Stream<OrientEdge> getIndexedEdges(OIndex<Object> index, Iterator<Object> valueIter) {
        return getIndexedElements(index, valueIter, OrientEdge::new);
    }

    private <ElementType extends OrientElement> Stream<ElementType> getIndexedElements(
            OIndex<Object> index,
            Iterator<Object> valuesIter,
            BiFunction<OrientGraph, OIdentifiable, ElementType> newElement) {
        return executeWithConnectionCheck(() -> {
            makeActive();

            if (index == null) {
                return Collections.<ElementType> emptyList().stream();
            } else {
                if (!valuesIter.hasNext()) {
                    return index.cursor().toValues().stream().map(id -> newElement.apply(this, id));
                } else {
                    Stream<Object> convertedValues = StreamUtils.asStream(valuesIter).map(value -> convertValue(index, value));
                    Stream<OIdentifiable> ids = convertedValues.flatMap(v -> lookupInIndex(index, v)).filter(r -> r != null);
                    Stream<ORecord> records = ids.map(id -> id.getRecord());
                    return records.map(r -> newElement.apply(this, getRawDocument(r)));
                }
            }
        });
    }

    private Stream<OIdentifiable> lookupInIndex(OIndex<Object> index, Object value) {
        Object fromIndex = index.get(value);
        if (fromIndex instanceof Iterable)
            return StreamUtils.asStream(((Iterable) fromIndex).iterator());
        else
            return Stream.of((OIdentifiable) fromIndex);
    }

    private OIndexManager getIndexManager() {
        return database.getMetadata().getIndexManager();
    }

    private OSchema getSchema() {
        return database.getMetadata().getSchema();
    }

    public Set<String> getIndexedKeys(String className) {
        Iterator<OIndex<?>> indexes = getIndexManager().getClassIndexes(className).iterator();
        HashSet<String> indexedKeys = new HashSet<>();
        indexes.forEachRemaining(index -> {
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
            return getIndexedKeys(OImmutableClass.VERTEX_CLASS_NAME);
        } else if (Edge.class.isAssignableFrom(elementClass)) {
            return getIndexedKeys(OImmutableClass.EDGE_CLASS_NAME);
        } else {
            throw new IllegalArgumentException("Class is not indexable: " + elementClass);
        }
    }

    public Set<String> getVertexIndexedKeys(final String label) {
        String className = labelToClassName(label, OImmutableClass.VERTEX_CLASS_NAME);
        OClass cls = getSchema().getClass(className);
        if (cls != null && cls.isSubClassOf(OImmutableClass.VERTEX_CLASS_NAME)) {
            return getIndexedKeys(className);
        }
        return new HashSet<String>();
    }

    public Set<String> getEdgeIndexedKeys(final String label) {
        String className = labelToClassName(label, OImmutableClass.EDGE_CLASS_NAME);
        OClass cls = getSchema().getClass(className);
        if (cls != null && cls.isSubClassOf(OImmutableClass.EDGE_CLASS_NAME)) {
            return getIndexedKeys(className);
        }
        return new HashSet<String>();
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        return executeWithConnectionCheck(() -> {
            makeActive();
            return elements(
                    OImmutableClass.EDGE_CLASS_NAME,
                    r -> new OrientEdge(this, getRawDocument(r)),
                    edgeIds);
        });
    }

    protected <A extends Element> Iterator<A> elements(String elementClass, Function<ORecord, A> toA, Object... elementIds) {
        boolean polymorphic = true;
        if (elementIds.length == 0) {
            // return all vertices as stream
            Iterator<ORecord> itty = new ORecordIteratorClass<>(database, database, elementClass, polymorphic);
            return asStream(itty).map(toA).iterator();
        } else {
            Stream<ORID> ids = Stream.of(elementIds).map(OrientGraph::createRecordId).peek(id -> checkId(id));
            Stream<ORecord> records = ids.filter(ORID::isValid).map(id -> (ORecord) id.getRecord()).filter(r -> r != null);
            return records.map(toA).iterator();
        }
    }

    private ORID checkId(ORID id) {
        if (!id.isValid())
            throw new IllegalArgumentException("Invalid id " + id);
        try {
            database.getRecordMetadata(id);
        } catch (IllegalArgumentException e) {
            // bummer, the API force me to break the chain =((
            // https://github.com/apache/incubator-tinkerpop/commit/34ec9e7f60f15b5dbfa684a8e96668d9bbcb6752#commitcomment-14235497
            throw Graph.Exceptions.elementNotFound(Edge.class, id);
        }
        return id;
    }

    protected static ORID createRecordId(Object id) {
        if (id instanceof ORecordId)
            return (ORecordId) id;
        if (id instanceof String)
            return new ORecordId((String) id);
        if (id instanceof OrientElement)
            return ((OrientElement) id).id();

        throw new IllegalArgumentException("Orient IDs have to be a String or ORecordId - you provided a " + id.getClass());
    }

    protected ODocument getRawDocument(ORecord record) {
        if (record == null) throw new NoSuchElementException();
        if (record instanceof OIdentifiable)
            record = record.getRecord();
        ODocument currentDocument = (ODocument) record;
        if (currentDocument.getInternalStatus() == ODocument.STATUS.NOT_LOADED)
            currentDocument.load();
        if (ODocumentInternal.getImmutableSchemaClass(currentDocument) == null)
            throw new IllegalArgumentException(
                    "Cannot determine the graph element type because the document class is null. Probably this is a projection, use the EXPAND() function");
        return currentDocument;
    }

    @Override
    public Transaction tx() {
        makeActive();
        return new OrientTransaction(this);
    }

    /**
     * (Blueprints Extension) Drops the database
     */
    public void drop() {
        makeActive();
        getRawDatabase().drop();
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

        final boolean txBegun = database.getTransaction().isActive();
        if (!txBegun) {
            database.begin();
            // TODO use setting to determine behavior settings.isUseLog()
            database.getTransaction().setUsingLog(true);
        }
    }

    public void commit() {
        makeActive();

        if (!features.graph().supportsTransactions()) {
            return;
        }
        if (database == null) {
            return;
        }

        database.commit();
        if (isAutoStartTx()) {
            begin();
        }
    }

    public void rollback() {
        makeActive();

        if (!features.graph().supportsTransactions()) {
            return;
        }

        if (database == null) {
            return;
        }

        database.rollback();
        if (isAutoStartTx()) {
            begin();
        }
    }

    public boolean isAutoStartTx() {
        // TODO use configuration to determine behavior
        return true;
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
    public void close() throws Exception {
        makeActive();
        boolean commitTx = true;
        String url = database.getURL();

        try {
            if (!database.isClosed() && commitTx) {
                final OStorage storage = database.getStorage();
                if (storage instanceof OAbstractPaginatedStorage) {
                    if (((OAbstractPaginatedStorage) storage).getWALInstance() != null)
                        database.commit();
                }

            }

        } catch (RuntimeException e) {
            OLogManager.instance().info(this, "Error during context close for db " + url, e);
            throw e;
        } catch (Exception e) {
            OLogManager.instance().error(this, "Error during context close for db " + url, e);
            throw new OException("Error during context close for db " + url, e);
        } finally {
            try {
                database.close();
            } catch (Exception e) {
                OLogManager.instance().error(this, "Error during context close for db " + url, e);
            }
        }
    }

    public String createVertexClass(final String label) {
        makeActive();
        String className = labelToClassName(label, OImmutableClass.VERTEX_CLASS_NAME);
        createClass(className, OImmutableClass.VERTEX_CLASS_NAME);
        return className;
    }

    public String createEdgeClass(final String label) {
        makeActive();
        String className = labelToClassName(label, OImmutableClass.EDGE_CLASS_NAME);
        createClass(className, OImmutableClass.EDGE_CLASS_NAME);
        return className;
    }

    public void createClass(final String className, final String superClassName) {
        makeActive();
        OClass superClass = database.getMetadata().getSchema().getClass(superClassName);
        if (superClass == null) {
            Collection<OClass> allClasses = database.getMetadata().getSchema().getClasses();
            throw new IllegalArgumentException("unable to find class " + superClassName + ". Available classes: " + allClasses);
        }
        createClass(className, superClass);
    }

    public void createClass(final String className, final OClass superClass) {
        makeActive();
        OSchemaProxy schema = database.getMetadata().getSchema();
        OClass cls = schema.getClass(className);
        if (cls == null) {
            try {
                schema.createClass(className, superClass);
            } catch (OException e) {
                throw new IllegalArgumentException(e);
            }
            OLogManager.instance().info(this, "created class '" + className + "' as subclass of '" + superClass + "'");
        } else {
            if (!cls.isSubClassOf(superClass)) {
                throw new IllegalArgumentException("unable to create class '" + className + "' as subclass of '" + superClass + "'. different super class.");
            }
        }
    }

    public ODatabaseDocumentTx getRawDatabase() {
        makeActive();
        return database;
    }

    protected <E> String getClassName(final Class<T> elementClass) {
        if (elementClass.isAssignableFrom(Vertex.class))
            return OImmutableClass.VERTEX_CLASS_NAME;
        else if (elementClass.isAssignableFrom(Edge.class))
            return OImmutableClass.EDGE_CLASS_NAME;
        throw new IllegalArgumentException("Class '" + elementClass + "' is neither a Vertex, nor an Edge");
    }

    protected void prepareIndexConfiguration(Configuration config) {
        String defaultIndexType = OClass.INDEX_TYPE.NOTUNIQUE.name();
        OType defaultKeyType = OType.STRING;
        String defaultClassName = null;
        String defaultCollate = null;
        ODocument defaultMetadata = null;

        if (!config.containsKey("type"))
            config.setProperty("type", defaultIndexType);
        if (!config.containsKey("keytype"))
            config.setProperty("keytype", defaultKeyType);
        if (!config.containsKey("class"))
            config.setProperty("class", defaultClassName);
        if (!config.containsKey("collate"))
            config.setProperty("collate", defaultCollate);
        if (!config.containsKey("metadata"))
            config.setProperty("metadata", defaultMetadata);
    }

    public <E extends Element> void createVertexIndex(final String key, final String label, final Configuration configuration) {
        String className = labelToClassName(label, OImmutableClass.VERTEX_CLASS_NAME);
        createVertexClass(label);
        createIndex(key, className, configuration);
    }

    public <E extends Element> void createEdgeIndex(final String key, final String label, final Configuration configuration) {
        String className = labelToClassName(label, OImmutableClass.EDGE_CLASS_NAME);
        createEdgeClass(label);
        createIndex(key, className, configuration);
    }

    private <E extends Element> void createIndex(final String key, String className, final Configuration configuration) {
        makeActive();

        prepareIndexConfiguration(configuration);

        OCallable<OClass, OrientGraph> callable = new OCallable<OClass, OrientGraph>() {
            @Override
            public OClass call(final OrientGraph g) {

                String indexType = configuration.getString("type");
                OType keyType = (OType) configuration.getProperty("keytype");
                String collate = configuration.getString("collate");
                ODocument metadata = (ODocument) configuration.getProperty("metadata");

                final ODatabaseDocumentTx db = getRawDatabase();
                final OSchema schema = db.getMetadata().getSchema();

                final OClass cls = schema.getClass(className);
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
        };
        execute(callable, "create key index on '", className, ".", key, "'");
    }

    public <RET> RET execute(final OCallable<RET, OrientGraph> iCallable, final String... iOperationStrings) throws RuntimeException {
        makeActive();

        if (OLogManager.instance().isWarnEnabled() && iOperationStrings.length > 0) {
            // COMPOSE THE MESSAGE
            final StringBuilder msg = new StringBuilder(256);
            for (String s : iOperationStrings)
                msg.append(s);

            // ASSURE PENDING TX IF ANY IS COMMITTED
            OLogManager.instance().warn(
                    this,
                    msg.toString());
        }
        return iCallable.call(this);
    }

    @SuppressWarnings({ "rawtypes", "unchecked" })
    @Override
    public <I extends Io> I io(Builder<I> builder) {
        return (I) Graph.super.io(builder.registry(OrientIoRegistry.getInstance()));
    }

    @Override
    public String toString() {
        return StringFactory.graphString(this, database.toString());
    }

}
