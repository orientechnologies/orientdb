package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordLazyList;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Collection;
import java.util.Iterator;

public final class OrientVertex extends OrientElement implements Vertex {

    public OrientVertex(final OrientGraph graph, final OIdentifiable rawElement) {
        super(graph, rawElement);
    }

    public OrientVertex(OrientGraph graph, String className) {
        super(graph, createRawElement(graph, className));
    }

    protected static ODocument createRawElement(OrientGraph graph, String className) {
        graph.createVertexClass(className);
        return new ODocument(className);
    }

    public Iterator<Edge> edges(final Direction direction, final String... edgeLabels) {
        throw new NotImplementedException();
    }

    public Iterator<Vertex> vertices(final Direction direction, final String... edgeLabels) {
        throw new NotImplementedException();
    }

    public <V> Iterator<VertexProperty<V>> properties(final String... propertyKeys) {
        Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
        return StreamUtils.asStream(properties).map(p ->
            (VertexProperty<V>) new OrientVertexProperty<>( p.key(), p.value(), (Vertex) p.element())
        ).iterator();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        super.property(key, value);
        return new OrientVertexProperty<>(key, value, this);
    }

    @Override
    public <V> VertexProperty<V> property(String key, V value, Object... keyValues) {
        throw new NotImplementedException();
    }

    @Override
    public <V> VertexProperty<V> property(
            final VertexProperty.Cardinality cardinality,
            final String key,
            final V value,
            final Object... keyValues) {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        String labelPart = "";

        if(!label().equals(OImmutableClass.VERTEX_CLASS_NAME))
            labelPart = "(" + label() + ")";
        return "v" + labelPart + "[" + id() + "]";
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex, Object... keyValues) {
        if (inVertex == null)
            throw new IllegalArgumentException("destination vertex is null");

//        if (checkDeletedInTx())
//            throw new IllegalStateException("The vertex " + getIdentity() + " has been deleted");
//
//        if (inVertex.checkDeletedInTx())
//            throw new IllegalStateException("The vertex " + inVertex.getIdentity() + " has been deleted");
//
//        final OrientBaseGraph graph = setCurrentGraphInThreadLocal();
//        if (graph != null)
//            graph.autoStartTransaction();
//
        final ODocument outDocument = getRawDocument();
        if( !outDocument.getSchemaClass().isSubClassOf("V") )
            throw new IllegalArgumentException("source record is not a vertex");

        final ODocument inDocument = ((OrientVertex)inVertex).getRawDocument();
        if( !inDocument.getSchemaClass().isSubClassOf("V") )
            throw new IllegalArgumentException("destination record is not a vertex");

        final OrientEdge edge;

        label = OrientGraphUtils.encodeClassName(label);

        //TODO: add edge edgetype support
//        final OrientEdgeType edgeType = graph.getCreateEdgeType(label);
//        OVERWRITE CLASS NAME BECAUSE ATTRIBUTES ARE CASE SENSITIVE
//        label = edgeType.getName();
//
        final String outFieldName = getConnectionFieldName(Direction.OUT, label);
        final String inFieldName = getConnectionFieldName(Direction.IN, label);

//         since the label for the edge can potentially get re-assigned
//         before being pushed into the OrientEdge, the null check has to go here.
        if (label == null)
            throw new IllegalStateException("label cannot be null");

        // CREATE THE EDGE DOCUMENT TO STORE FIELDS TOO
        edge = new OrientEdge(graph, label/*, fields*/);
        //TODO: support inMemoryReferences
//        if (settings.isKeepInMemoryReferences())
//            edge.getRecord().fields(OrientBaseGraph.CONNECTION_OUT, rawElement.getIdentity(), OrientBaseGraph.CONNECTION_IN, inDocument.getIdentity());
//        else
        getRawDocument().fields(OrientGraphUtils.CONNECTION_OUT, rawElement, OrientGraphUtils.CONNECTION_IN, inDocument);

        OIdentifiable from = edge.getRawDocument();
        OIdentifiable to = edge.getRawDocument();

        //TODO: support inMemoryReferences
//        if (settings.isKeepInMemoryReferences()) {
//            // USES REFERENCES INSTEAD OF DOCUMENTS
//            from = from.getIdentity();
//            to = to.getIdentity();
//        }

        createLink(outDocument, to, outFieldName);
        createLink(inDocument, from, inFieldName);

        //TODO: support clusters
//      edge.save(iClusterName);
        edge.save();
        inDocument.save();
        outDocument.save();
        return edge;
    }

    public static String getConnectionFieldName(final Direction iDirection, final String iClassName) {
        if (iDirection == null || iDirection == Direction.BOTH)
            throw new IllegalArgumentException("Direction not valid");

        // TODO: removed support for VertexFieldsForEdgeLabels here
//        if (useVertexFieldsForEdgeLabels) {
//            // PREFIX "out_" or "in_" TO THE FIELD NAME
//            final String prefix = iDirection == Direction.OUT ? CONNECTION_OUT_PREFIX : CONNECTION_IN_PREFIX;
//            if (iClassName == null || iClassName.isEmpty() || iClassName.equals(OrientEdgeType.CLASS_NAME))
//                return prefix;
//
//            return prefix + iClassName;
//        } else
            // "out" or "in"
        return iDirection == Direction.OUT ? OrientGraphUtils.CONNECTION_OUT : OrientGraphUtils.CONNECTION_IN;
    }

    // this ugly code was copied from the TP2 implementation
    public Object createLink(final ODocument iFromVertex, final OIdentifiable iTo, final String iFieldName) {
        final Object out;
        OType outType = iFromVertex.fieldType(iFieldName);
        Object found = iFromVertex.field(iFieldName);

        final OClass linkClass = ODocumentInternal.getImmutableSchemaClass(iFromVertex);
        if (linkClass == null)
            throw new IllegalArgumentException("Class not found in source vertex: " + iFromVertex);

        final OProperty prop = linkClass.getProperty(iFieldName);
        final OType propType = prop != null && prop.getType() != OType.ANY ? prop.getType() : null;

        if (found == null) {
            //TODO: support these graph properties
//            if (iGgraph.isAutoScaleEdgeType() && (prop == null || propType == OType.LINK || "true".equalsIgnoreCase(prop.getCustom("ordered")))) {
//                // CREATE ONLY ONE LINK
//                out = iTo;
//                outType = OType.LINK;
//            } else if (propType == OType.LINKLIST || (prop != null && "true".equalsIgnoreCase(prop.getCustom("ordered")))) {
//                final Collection coll = new ORecordLazyList(iFromVertex);
//                coll.add(iTo);
//                out = coll;
//                outType = OType.LINKLIST;
           /* } else */ if (propType == null || propType == OType.LINKBAG) {
                final ORidBag bag = new ORidBag();
                bag.add(iTo);
                out = bag;
                outType = OType.LINKBAG;
            } else
                throw new IllegalStateException("Type of field provided in schema '" + prop.getType() + "' can not be used for link creation.");
//
        } else if (found instanceof OIdentifiable) {
            if (prop != null && propType == OType.LINK)
                throw new IllegalStateException("Type of field provided in schema '" + prop.getType() + "' can not be used for creation to hold several links.");
//
            if (prop != null && "true".equalsIgnoreCase(prop.getCustom("ordered"))) {
                final Collection coll = new ORecordLazyList(iFromVertex);
                coll.add(found);
                coll.add(iTo);
                out = coll;
                outType = OType.LINKLIST;
            } else {
                final ORidBag bag = new ORidBag();
                bag.add((OIdentifiable) found);
                bag.add(iTo);
                out = bag;
                outType = OType.LINKBAG;
            }
        } else if (found instanceof ORidBag) {
            // ADD THE LINK TO THE COLLECTION
            out = null;
            ((ORidBag) found).add(iTo);
        } else if (found instanceof Collection<?>) {
            // USE THE FOUND COLLECTION
            out = null;
            ((Collection<Object>) found).add(iTo);
        } else
            throw new IllegalStateException("Relationship content is invalid on field " + iFieldName + ". Found: " + found);

        if (out != null)
            // OVERWRITE IT
            iFromVertex.field(iFieldName, out, outType);

        return out;
    }


}
