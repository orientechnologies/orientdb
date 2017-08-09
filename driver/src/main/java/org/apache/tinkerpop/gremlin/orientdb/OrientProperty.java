package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.record.OElement;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

import java.util.NoSuchElementException;

public class OrientProperty<V> implements Property<V> {
    protected String key;
    protected V value;
    protected Object wrappedValue;
    protected OrientElement element;

    public OrientProperty(String key, V value, OrientElement element) {
        this.key = key;
        this.value = value;
        this.element = element;
        this.wrappedValue = wrapIntoGraphElement(value);

    }

    private Object wrapIntoGraphElement(V value) {
        Object result = value;
        if (result instanceof OElement) {
            if (((OElement) result).isVertex()) {
                result = new OrientVertex(element.getGraph(), ((OElement) result).asVertex().get());
            } else if (((OElement) value).isEdge()) {
                result = new OrientEdge(element.getGraph(), ((OElement) result).asEdge().get());
            }
        }
        return result;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return (V) wrappedValue;
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
        OElement doc = element.getRawElement();
        doc.removeProperty(key);
        doc.save();
        this.value = null;
        wrappedValue = null;
    }

    @Override
    public String toString() {
        return StringFactory.propertyString(this);
    }

    @Override
    public final boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }

    @Override
    public int hashCode() {
        return ElementHelper.hashCode(this);
    }

}
