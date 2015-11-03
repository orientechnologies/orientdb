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
        this.value = null;
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "[" + key + "=" + value + "]";
    }

	@Override
	public int hashCode() {
		final int prime = 73;
		int result = 1;
		result = prime * result + ((element == null) ? 0 : element.hashCode());
		result = prime * result + ((key == null) ? 0 : key.hashCode());
		result = prime * result + ((value == null) ? 0 : value.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OrientProperty other = (OrientProperty) obj;
		if (element == null) {
			if (other.element != null)
				return false;
		} else if (!element.equals(other.element))
			return false;
		if (key == null) {
			if (other.key != null)
				return false;
		} else if (!key.equals(other.key))
			return false;
		if (value == null) {
			if (other.value != null)
				return false;
		} else if (!value.equals(other.value))
			return false;
		return true;
	}

}
