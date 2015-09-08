package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.tinkerpop.gremlin.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Arrays;
import java.util.Iterator;

public class OrientEdge extends OrientElement implements Edge {
    protected OIdentifiable   vOut;
    protected OIdentifiable   vIn;
    protected String          label;

    public OrientEdge(OrientGraph graph, OIdentifiable rawElement) {
        super(graph, rawElement);
    }

    public OrientEdge(OrientGraph graph, String className) {
        super(graph, createRawElement(graph, className));
    }

    protected OrientEdge(final OrientGraph graph, final OIdentifiable out, final OIdentifiable in, final String iLabel) {
        super(graph, null);
        vOut = out;
        vIn = in;
        label = iLabel;
    }

    public OrientEdge(OrientGraph graph, OIdentifiable rawEdge, String label) {
        super(graph, rawEdge);
        this.label = label;
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
        throw new NotImplementedException();
    }

    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
        return StreamUtils.asStream(properties).map(p ->
            (Property<V>) new OrientProperty<>(p.key(), p.value(), p.element())
        ).iterator();
    }

    public OrientVertex getVertex(final Direction direction) {
        if (direction.equals(Direction.OUT))
            return new OrientVertex(graph, getOutVertex());
        else if (direction.equals(Direction.IN))
            return new OrientVertex(graph, getInVertex());
        else
            throw new IllegalArgumentException("direction " + direction + " is not supported!");
    }

    public OIdentifiable getOutVertex() {
        if (vOut != null)
            // LIGHTWEIGHT EDGE
            return vOut;

        final ODocument doc = getRawDocument();
        if (doc == null)
            return null;

//        if (settings != null && settings.isKeepInMemoryReferences())
            // AVOID LAZY RESOLVING+SETTING OF RECORD
//            return doc.rawField(OrientGraphUtils.CONNECTION_OUT);
//        else
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
        System.out.println(doc);
        System.out.println(Arrays.asList(doc.fieldNames()));

        if (doc == null)
            return null;

//        if (settings != null && settings.isKeepInMemoryReferences())
            // AVOID LAZY RESOLVING+SETTING OF RECORD
//            return doc.rawField(OrientGraphUtils.CONNECTION_IN);
//        else
            return doc.field(OrientGraphUtils.CONNECTION_IN);
    }

    @Override
    public String toString() {
        String labelPart = "";
        if(!label().equals(OImmutableClass.EDGE_CLASS_NAME))
            labelPart = "(" + label() + ")";

        String verticesPart = "";
//        String verticesPart = "|| In: " + getInVertex() + " || Out: " + getOutVertex();
//        String verticesPart = "[" + getOutVertex().getIdentity() + "->" + getInVertex().getIdentity() + "]";

        return "e" + labelPart + "[" + id() + "]" + verticesPart;
    }
}
