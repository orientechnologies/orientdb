package org.apache.tinkerpop.gremlin.orientdb.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;
import java.util.stream.Stream;

import org.apache.commons.lang.NotImplementedException;


public class OrientElement implements Element {
    protected OIdentifiable rawElement;
    protected OrientGraph graph;

    public OrientElement(final OrientGraph graph, final OIdentifiable rawElement) {
        this.graph = graph;
        this.rawElement = rawElement;
    }

    public Object id() {
        return rawElement.getIdentity();
    }

    public String label() {
        throw new NotImplementedException();
    }

    public Graph graph() {
        return graph;
    }

    public <V> Property<V> property(final String key, final V value) {
        if (rawElement instanceof ODocument) {
            ODocument doc = (ODocument) rawElement;
            doc.field(key, value);
            doc.save();
        }

        return new OrientProperty<>(key, value, this);
    }

    public void remove() {
        throw new NotImplementedException();
    }

    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        ODocument raw = rawElement.getRecord();
        Map<String, Object> properties = raw.toMap();
        HashSet<String> keys = new HashSet<>(Arrays.asList(propertyKeys));

        Stream<Map.Entry<String, Object>> entries = StreamUtils.asStream(properties.entrySet().iterator());
        if (keys.size() > 0) entries = entries.filter(entry -> keys.contains(entry));

        Stream<OrientProperty<V>> propertyStream = entries.map(entry -> new OrientProperty<>(entry.getKey(), (V) entry.getValue(), this));
        return propertyStream.iterator();
    }

}
