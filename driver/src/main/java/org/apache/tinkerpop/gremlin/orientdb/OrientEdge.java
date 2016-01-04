package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.db.record.ORecordElement;
import com.orientechnologies.orient.core.db.record.ridbag.ORidBag;
import com.orientechnologies.orient.core.record.impl.ODocument;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

public class OrientEdge extends OrientElement implements Edge {

    private static final List<String> INTERNAL_FIELDS = Arrays.asList("@rid", "@class", "in", "out");

    protected OIdentifiable vOut;
    protected OIdentifiable vIn;
    protected String label;

    public OrientEdge(OrientGraph graph, OIdentifiable rawElement, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
        super(graph, rawElement);
        vOut = checkNotNull(out);
        vIn = checkNotNull(in);
        label = checkNotNull(iLabel);
    }

    public OrientEdge(OrientGraph graph, String className, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
        this(graph, createRawElement(graph, className), out, in, iLabel);
    }

    public OrientEdge(OrientGraph graph, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
        this(graph, (OIdentifiable) null, out, in, iLabel);
    }

    public OrientEdge(OrientGraph graph, ODocument rawDocument, String label) {
        this(graph, rawDocument, rawDocument.field("out", OIdentifiable.class), rawDocument.field("in", OIdentifiable.class), label);
    }

    public OrientEdge(OrientGraph graph, ODocument rawDocument) {
        this(graph, rawDocument, rawDocument.getClassName());
    }

    public static OIdentifiable getConnection(final ODocument iEdgeRecord, final Direction iDirection) {
        return iEdgeRecord.rawField(iDirection == Direction.OUT ? OrientGraphUtils.CONNECTION_OUT : OrientGraphUtils.CONNECTION_IN);
    }

    protected static ODocument createRawElement(OrientGraph graph, String className) {
        graph.createEdgeClass(className);
        return new ODocument(className);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        switch (direction) {
        case OUT:
            return graph.vertices(vOut.getIdentity());
        case IN:
            return graph.vertices(vIn.getIdentity());
        case BOTH:
        default:
            return graph.vertices(vOut.getIdentity(), vIn.getIdentity());
        }
    }

    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
        return StreamUtils.asStream(properties).filter(p -> !INTERNAL_FIELDS.contains(p.key())).map(p -> (Property<V>) p).iterator();
    }

    public OrientVertex getVertex(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return new OrientVertex(graph, getOutVertex());
        else if (direction.equals(Direction.IN))
            return new OrientVertex(graph, getInVertex());
        else
            throw new IllegalArgumentException("direction " + direction + " is not supported!");
    }

    public void remove() {
        ODocument doc = getRawDocument();
        if (doc.getInternalStatus() == ORecordElement.STATUS.NOT_LOADED) {
            doc.load();
        }

        removeLink(Direction.IN);
        removeLink(Direction.OUT);
        doc.getDatabase().delete(doc.getIdentity());
    }

    @SuppressWarnings("unchecked")
    private void removeLink(Direction direction) {
        final String fieldName = OrientVertex.getConnectionFieldName(direction, this.label());
        ODocument doc = this.getVertex(direction).getRawDocument();
        Object found = doc.field(fieldName);
        if (found instanceof ORidBag) {
            ORidBag bag = (ORidBag) found;
            bag.remove(this.getRawElement());
            if (bag.size() == 0) doc.removeField(fieldName);
        } else if (found instanceof Collection<?>) {
            ((Collection<Object>) found).remove(this.getRawElement());
            if (((Collection<Object>) found).size() == 0) doc.removeField(fieldName);
        } else
            throw new IllegalStateException("Relationship content is invalid on field " + fieldName + ". Found: " + found);
        doc.save();
    }

    public OIdentifiable getOutVertex() {
        if (vOut != null)
            // LIGHTWEIGHT EDGE
            return vOut;

        final ODocument doc = getRawDocument();
        if (doc == null)
            return null;

        return doc.field(OrientGraphUtils.CONNECTION_OUT);
    }

    /**
     * (Blueprints Extension) Returns the incoming vertex in form of record.
     */
    public OIdentifiable getInVertex() {
        if (vIn != null)
            // LIGHTWEIGHT EDGE
            return vIn;

        final ODocument doc = getRawDocument();
        if (doc == null)
            return null;

        return doc.field(OrientGraphUtils.CONNECTION_IN);
    }

    @Override
    public String toString() {
        return StringFactory.edgeString(this);
    }

}
