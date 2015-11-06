package org.apache.tinkerpop.gremlin.orientdb;

import java.util.Iterator;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class OrientVertexProperty<V> extends OrientProperty<V> implements VertexProperty<V> {
    protected OrientVertex vertex;

    public OrientVertexProperty(String key, V value, OrientVertex vertex) {
        super(key, value, vertex);
        this.vertex = vertex;
    }

    @Override
    public Object id() {//XXX make sure this is the correct ID
        return vertex.id() + ":" + key;
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw VertexProperty.Exceptions.metaPropertiesNotSupported();
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public final boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

}
