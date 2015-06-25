package org.apache.tinkerpop.gremlin.orientdb.structure;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import org.apache.commons.lang.NotImplementedException;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;


public final class OrientVertex extends OrientElement implements Vertex {

    public OrientVertex(final OrientGraph graph, final OIdentifiable rawElement) {
        super(graph, rawElement);
    }

    public OrientVertex(OrientGraph graph, String className) {
        super(graph, className);
    }

    public Edge addEdge(final String label, final Vertex inVertex, final Object... keyValues) {
        throw new NotImplementedException();
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
}
