package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.tinkerpop.gremlin.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

public class OrientEdge extends OrientElement implements Edge {
    public OrientEdge(OrientGraph graph, OIdentifiable rawElement) {
        super(graph, rawElement);
    }

    public OrientEdge(OrientGraph graph, String className) {
        super(graph, createRawElement(graph, className));
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

    @Override
    public String toString() {
        String labelPart = "";

        if(!label().equals(OImmutableClass.EDGE_CLASS_NAME))
            labelPart = "(" + label() + ")";
        return "e" + labelPart + "[" + id() + "]";
    }
}
