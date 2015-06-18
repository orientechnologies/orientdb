package org.apache.tinkerpop.gremlin.orientdb.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.NoSuchElementException;

public class OrientVertexProperty<V> implements VertexProperty<V> {

    private String key;
    private V value;
    private Vertex vertex;

    public OrientVertexProperty(String key, V value, Vertex vertex) {
        this.key = key;
        this.value = value;
        this.vertex = vertex;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return vertex;
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
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
}
