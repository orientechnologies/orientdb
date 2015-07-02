package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.ODatabaseFactory;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.logging.Logger;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.orientdb.StreamUtils.asStream;

@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
public final class OrientGraph implements Graph {
    public static String CONFIG_URL = "orient-url";
    public static String CONFIG_USER = "orient-user";
    public static String CONFIG_PASS = "orient-pass";
    public static String CONFIG_CREATE = "orient-create";
    public static String CONFIG_OPEN = "orient-open";

    protected Logger log = Logger.getLogger(getClass().getSimpleName());
    protected ODatabaseDocumentTx database;

    public static OrientGraph open(final Configuration configuration) {
        return new OrientGraph(configuration);
    }

    public OrientGraph(Configuration config) {
        this.database = getDatabase(
            config.getString(CONFIG_URL, "memory:test-" + Math.random()),
            config.getString(CONFIG_USER, "admin"),
            config.getString(CONFIG_PASS, "admin"),
            config.getBoolean(CONFIG_CREATE, true),
            config.getBoolean(CONFIG_OPEN, true));
        makeActive();
    }

    public void makeActive() {
//        activeGraph.set(this);

        final ODatabaseDocument tlDb = ODatabaseRecordThreadLocal.INSTANCE.getIfDefined();
        if (database != null && tlDb != database)
            ODatabaseRecordThreadLocal.INSTANCE.set(database);
    }

    /**
     * @param create
     *          if true automatically creates database if database with given URL does not exist
     * @param open
     *          if true automatically opens the database
     */
    protected ODatabaseDocumentTx getDatabase(String url, String user, String password, boolean create, boolean open) {
        final ODatabaseDocumentTx db = new ODatabaseFactory().createDatabase("graph", url);
        if (!db.getURL().startsWith("remote:") && !db.exists()) {
            if (create) db.create();
            else if (open) throw new ODatabaseException("Database '" + url + "' not found");
        } else if (open) db.open(user, password);

        return db;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        makeActive();
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        String label = ElementHelper.getLabelValue(keyValues).orElse(OImmutableClass.VERTEX_CLASS_NAME);
        OrientVertex vertex = new OrientVertex(this, label);
        ElementHelper.attachProperties(vertex, keyValues);

        vertex.save();
        return vertex;
    }

    public Object executeSql(String sql) {
        makeActive();
        OCommandRequest command = database.command(new OCommandSQL(sql));
        return command.execute();
    }
    public Object executeCommand(OCommandRequest command) {
        return command.execute();
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
        makeActive();
        return elements(
                OImmutableClass.VERTEX_CLASS_NAME,
                r -> new OrientVertex(this, getRawDocument(r)),
                vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        makeActive();
        return elements(
                OImmutableClass.EDGE_CLASS_NAME,
                r -> new OrientEdge(this, getRawDocument(r)),
                edgeIds);
    }


    protected <A extends Element> Iterator<A> elements(String elementClass, Function<ORecord, A> toA, Object... elementIds) {
        boolean polymorphic = true;
        if (elementIds.length == 0) {
            // return all vertices as stream
            Iterator<ORecord> itty = new ORecordIteratorClass<>(database, database, elementClass, polymorphic);
            return asStream(itty).map(toA).iterator();
        } else {
            Stream<ORecordId> ids = Stream.of(elementIds).map(OrientGraph::createRecordId);
            Stream<ORecord> records = ids.filter(id -> id.isValid()).map(id -> (ORecord) id.getRecord()).filter(r -> r != null);
            return records.map(toA).iterator();
        }
    }

    protected static ORecordId createRecordId(Object id) {
        if (id instanceof ORecordId)
            return (ORecordId)id;
        else if (id instanceof String)
            return new ORecordId((String)id);
        else
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
        throw new NotImplementedException();
    }

    @Override
    public Variables variables() {
        makeActive();
        throw new NotImplementedException();
    }

    @Override
    public Configuration configuration() {
        makeActive();
        throw new NotImplementedException();
    }

    @Override
    public void close() throws Exception {
        makeActive();
        database.close();
    }

    public void createVertexClass(final String className) {
        makeActive();
        createClass(className, OImmutableClass.VERTEX_CLASS_NAME);
    }

    public void createEdgeClass(final String className) {
        makeActive();
        createClass(className, OImmutableClass.EDGE_CLASS_NAME);
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
        if (schema.getClass(className) == null) {
            schema.createClass(className, superClass);
            log.info("created class '" + className + "' as subclass of '" + superClass + "'");
        }
    }

    public ODatabaseDocumentTx getRawDatabase() {
        makeActive();
        return database;
    }

    /**
     * Returns the persistent class for type iTypeName as OrientEdgeType instance.
     *
     * @param iTypeName
     *          Edge class name
     */
    public final OrientEdgeType getEdgeType(final String iTypeName) {
        makeActive();
        final OClass cls = getRawDatabase().getMetadata().getSchema().getClass(iTypeName);
        if (cls == null)
            return null;

        OrientEdgeType.checkType(cls);
        return new OrientEdgeType(this, cls);
    }


}
