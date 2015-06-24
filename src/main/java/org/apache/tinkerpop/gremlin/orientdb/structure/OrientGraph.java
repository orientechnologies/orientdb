package org.apache.tinkerpop.gremlin.orientdb.structure;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.orientdb.structure.StreamUtils.asStream;


public final class OrientGraph implements Graph {

//    public static final Logger LOGGER = LoggerFactory.getLogger(OrientGraph.class);
    protected ODatabaseDocumentTx database;

    public OrientGraph(ODatabaseDocumentTx iDatabase) {
        this.database = iDatabase;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        OrientVertex v = new OrientVertex(this);
        v.save();
        return v;
    }

    public Object executeSql(String sql) {
        OCommandRequest command = database.command(new OCommandSQL(sql));
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
        //TODO: limit on vertexIds
        boolean polymorphic = true;
        // String iClassName = OrientVertexType.CLASS_NAME;
        String elementClass = "V";

        if (vertexIds.length == 0) {
            // return all vertices as stream
            Iterator<ORecord> itty = new ORecordIteratorClass<>(database, database, elementClass, polymorphic);
            return asStream(itty).map(r -> toVertex(r)).iterator();
        } else {
            Stream<ORecordId> ids = Stream.of(vertexIds).map(v -> new ORecordId(v.toString()));
            Stream<ORecord> records = ids.filter(id -> id.isValid()).map(id -> (ORecord) id.getRecord()).filter(r -> r != null);
            return records.map(record -> (Vertex) new OrientVertex(this, record)).iterator();
        }
    }

    private Vertex toVertex(ORecord record) {
        // taken mostly from old blueprints driver, could be done nicer I believe
        OrientElement currentElement = null;

        if (record == null) throw new NoSuchElementException();

        if (record instanceof OIdentifiable)
            record = record.getRecord();

        if (record instanceof ODocument) {
            final ODocument currentDocument = (ODocument) record;

            if (currentDocument.getInternalStatus() == ODocument.STATUS.NOT_LOADED)
                currentDocument.load();

            if (ODocumentInternal.getImmutableSchemaClass(currentDocument) == null)
                throw new IllegalArgumentException(
                    "Cannot determine the graph element type because the document class is null. Probably this is a projection, use the EXPAND() function");

            // if (ODocumentInternal.getImmutableSchemaClass(currentDocument).isSubClassOf(graph.getEdgeBaseType()))
            //   currentElement = new OrientEdge(graph, currentDocument);
            // else
            currentElement = new OrientVertex(this, currentDocument);
        }

        return (Vertex) currentElement;
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
        throw new NotImplementedException();
    }

    @Override
    public Transaction tx() {
        throw new NotImplementedException();
    }

    @Override
    public Variables variables() {
        throw new NotImplementedException();
    }

    @Override
    public Configuration configuration() {
        throw new NotImplementedException();
    }

    @Override
    public void close() throws Exception {
        throw new NotImplementedException();
    }

    public void createVertexClass(final String className) {
        createClass(className, "V");
    }

    public void createEdgeClass(final String className) {
        createClass(className, "E");
    }

    public void createClass(final String className, final String superClassName) {
//        makeActive();
        OClass cls = database.getMetadata().getSchema().getClass(superClassName);
        createClass(className, cls);
    }

    public void createClass(final String className, final OClass superClass) {
//        makeActive();
        database.getMetadata().getSchema().createClass(className, superClass);
    }

}
