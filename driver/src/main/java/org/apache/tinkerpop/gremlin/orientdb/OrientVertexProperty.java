package org.apache.tinkerpop.gremlin.orientdb;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;

public class OrientVertexProperty<V> extends OrientProperty<V> implements VertexProperty<V> {

    public OrientVertexProperty(Property<V> property, OrientVertex vertex) {
        super(property.key(), property.value(), vertex);
    }

    public OrientVertexProperty(String key, V value, OrientVertex vertex) {
        super(key, value, vertex);
    }

    @Override
    public ORID id() {
        return getMetadataDocument().getIdentity();
    }

    @Override
    public <U> Property<U> property(String key, U value) {
        if (T.id.equals(key))
            throw VertexProperty.Exceptions.userSuppliedIdsNotSupported();

        ODocument metadata = getMetadataDocument();

        metadata.field(key, value);
        return new OrientVertexPropertyProperty<>(key, value, this);
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        if (!hasMetadataDocument())
            return Collections.emptyIterator();

        Map<String, Object> properties = getMetadataDocument().toMap();
        HashSet<String> keys = new HashSet<>(Arrays.asList(propertyKeys));

        Stream<Map.Entry<String, Object>> entries = StreamUtils.asStream(properties.entrySet().iterator());
        if (keys.size() > 0) {
            entries = entries.filter(entry -> keys.contains(entry.getKey()));
        }

        @SuppressWarnings("unchecked")
        Stream<Property<U>> propertyStream = entries
                .filter(entry -> !entry.getKey().startsWith("@rid"))
                .map(entry -> new OrientVertexPropertyProperty<>(entry.getKey(), (U) entry.getValue(), this));
        return propertyStream.iterator();
    }

    private boolean hasMetadataDocument() {
        return element.getRawDocument().field(metadataKey()) != null;
    }

    public void removeMetadata(String key) {
        ODocument metadata = getMetadataDocument();
        metadata.removeField(key);
        if (metadata.fields() == 0)
            element.getRawDocument().removeField(metadataKey());
    }

    ODocument getMetadataDocument() {
        ODocument metadata = element.getRawDocument().field(metadataKey());
        if (metadata == null) {
            metadata = new ODocument();
            ODocument vertexDocument = element.getRawDocument();
            vertexDocument.field(metadataKey(), metadata, OType.EMBEDDED);
            vertexDocument.save();
            metadata.save();
        }
        return metadata;
    }

    @Override
    public void remove() {
        super.remove();

        element.getRawDocument().removeField(metadataKey());
    }

    private String metadataKey() {
        return "_meta_" + key;
    }

    @Override
    public Vertex element() {
        return (Vertex) element;
    }

}
