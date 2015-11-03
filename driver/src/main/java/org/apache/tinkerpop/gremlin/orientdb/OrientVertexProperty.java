package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.Iterator;

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
	public int hashCode() {
		final int prime = 73;
		int result = super.hashCode();
		result = prime * result + ((vertex == null) ? 0 : vertex.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (!super.equals(obj))
			return false;
		if (getClass() != obj.getClass())
			return false;
		OrientVertexProperty other = (OrientVertexProperty) obj;
		if (vertex == null) {
			if (other.vertex != null)
				return false;
		} else if (!vertex.equals(other.vertex))
			return false;
		return true;
	}

}
