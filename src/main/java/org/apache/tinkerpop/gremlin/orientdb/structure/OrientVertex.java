package org.apache.tinkerpop.gremlin.orientdb.structure;

import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.apache.tinkerpop.gremlin.structure.*;
import org.apache.commons.lang.NotImplementedException;
import java.util.*;
import java.util.stream.Stream;


public final class OrientVertex extends OrientElement implements Vertex {

    protected OrientVertex vertex = this;

    public OrientVertex(final OrientGraph graph, final OIdentifiable rawElement) {
        super(graph, rawElement);
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
        ODocument raw = rawElement.getRecord();
        Map<String, Object> properties = raw.toMap();
        HashSet<String> keys = new HashSet<>(Arrays.asList(propertyKeys));

        Stream<Map.Entry<String, Object>> entries = StreamUtils.asStream(properties.entrySet().iterator());
        if (keys.size() > 0) entries = entries.filter(entry -> keys.contains(entry));

        Stream<VertexProperty<V>> propertyStream = entries.map(entry -> new OrientVertexProperty<>(entry.getKey(), (V) entry.getValue(), vertex));
        return propertyStream.iterator();
    }

    @Override
    public <V> VertexProperty<V> property(final String key, final V value) {
        throw new NotImplementedException();
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

    public String toString() {
        // final String clsName = rawElement.getClassName();
        return "v[" + id() + "]";
    }
}
