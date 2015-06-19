package org.apache.tinkerpop.gremlin.orientdb.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

public class OrientVertexProperty<V> extends OrientProperty<V> implements VertexProperty<V> {
    protected Vertex vertex;

    public OrientVertexProperty(String key, V value, Vertex vertex) {
        super(key, value, vertex);
        this.vertex = vertex;
    }

    @Override
    public Object id() {
        throw new NotImplementedException();
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        throw new NotImplementedException();
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        throw new NotImplementedException();
    }

    @Override
    public Vertex element() {
        return vertex;
    }
}
