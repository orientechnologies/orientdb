package org.apache.tinkerpop.gremlin.orientdb.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;
import java.util.NoSuchElementException;

public class OrientProperty<V> implements Property<V> {
    protected String key;
    protected V value;
    protected Element element;

    public OrientProperty(String key, V value, Element element) {
        this.key = key;
        this.value = value;
        this.element = element;
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
        return value != null;
    }

    @Override
    public Element element() {
        return this.element;
    }

    @Override
    public void remove() {
        throw new NotImplementedException();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + key + "=" + value + "]";
    }
}
