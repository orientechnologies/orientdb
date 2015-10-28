package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.NoSuchElementException;

public class OrientProperty<V> implements Property<V> {
    protected String key;
    protected V value;
    protected OrientElement element;

    public OrientProperty(String key, V value, OrientElement element) {
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
        ODocument doc = element.getRawDocument();
        doc.removeField(key);
        doc.save();
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + key + "=" + value + "]";
    }
}
