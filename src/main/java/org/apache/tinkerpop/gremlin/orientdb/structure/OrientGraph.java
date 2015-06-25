package org.apache.tinkerpop.gremlin.orientdb.structure;

import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OSchemaProxy;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.sql.OCommandSQL;
import org.apache.commons.configuration.Configuration;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.stream.Stream;

import static org.apache.tinkerpop.gremlin.orientdb.structure.StreamUtils.asStream;


public final class OrientGraph implements Graph {

    protected ODatabaseDocumentTx database;

    public OrientGraph(ODatabaseDocumentTx iDatabase) {
        this.database = iDatabase;
    }

    @Override
    public Vertex addVertex(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        String label = ElementHelper.getLabelValue(keyValues).orElse(OImmutableClass.VERTEX_CLASS_NAME);
        OrientVertex vertex = new OrientVertex(this, label);
        ElementHelper.attachProperties(vertex, keyValues);

        vertex.save();
        return vertex;
    }

    public Object executeSql(String sql) {
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
        return elements(
                OImmutableClass.VERTEX_CLASS_NAME,
                r -> new OrientVertex(this, getRawDocument(r)),
                vertexIds);
    }

    @Override
    public Iterator<Edge> edges(Object... edgeIds) {
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
            Stream<ORecordId> ids = Stream.of(elementIds).map(v -> new ORecordId(v.toString()));
            Stream<ORecord> records = ids.filter(id -> id.isValid()).map(id -> (ORecord) id.getRecord()).filter(r -> r != null);
            return records.map(toA).iterator();
        }
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
        createClass(className, OImmutableClass.VERTEX_CLASS_NAME);
    }

    public void createEdgeClass(final String className) {
        createClass(className, OImmutableClass.EDGE_CLASS_NAME);
    }

    public void createClass(final String className, final String superClassName) {
//        makeActive();
        OClass superClass = database.getMetadata().getSchema().getClass(superClassName);
        createClass(className, superClass);
    }

    public void createClass(final String className, final OClass superClass) {
//        makeActive();
        OSchemaProxy schema = database.getMetadata().getSchema();
        if (schema.getClass(className) == null) {
            schema.createClass(className, superClass);
        }
    }

    public ODatabaseDocumentTx getRawDatabase() {
        return database;
    }

}
