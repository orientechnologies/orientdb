package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;


public class OrientElement implements Element {
    protected OIdentifiable rawElement;
    protected OrientGraph graph;

    public OrientElement(final OrientGraph graph, final OIdentifiable rawElement) {
        if (rawElement == null)
            throw new IllegalArgumentException("rawElement must not be null!");
        this.graph = graph;
        this.rawElement = rawElement;
    }

    public Object id() {
        return rawElement.getIdentity();
    }

    public String label() {
        String internalClassName = getRawDocument().getClassName();
        // User labels on edges/vertices are prepended with E_ or V_ . The user should not see that.
        return internalClassName.length() == 1 ? internalClassName : internalClassName.substring(2);
    }

    public Graph graph() {
        return graph;
    }

    public <V> Property<V> property(final String key, final V value) {
        ODocument doc = getRawDocument();
        doc.field(key, value);
        doc.save();
        return new OrientProperty<>(key, value, this);
    }

    public void remove() {
        getRawDocument().delete();
    }

    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        ODocument record = rawElement.getRecord();
        if (record == null)
            record = new ODocument();

        Map<String, Object> properties = record.toMap();
        HashSet<String> keys = new HashSet<>(Arrays.asList(propertyKeys));

        Stream<Map.Entry<String, Object>> entries = StreamUtils.asStream(properties.entrySet().iterator());
        if (keys.size() > 0) {
            entries = entries.filter(entry -> keys.contains(entry.getKey()));
        }

        Stream<OrientProperty<V>> propertyStream = entries.map(entry -> new OrientProperty<>(entry.getKey(), (V) entry.getValue(), this));
        return propertyStream.iterator();
    }

    public void save() {
        ((ODocument)rawElement).save();
    }

    public ODocument getRawDocument() {
        if (!(rawElement instanceof ODocument))
            rawElement = new ODocument(rawElement.getIdentity());
        return (ODocument) rawElement;
    }

    public OrientGraph getGraph() {
        return graph;
    }

    public OIdentifiable getRawElement() {
        return rawElement;
    }
}
