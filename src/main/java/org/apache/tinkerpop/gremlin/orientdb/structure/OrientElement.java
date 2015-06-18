package org.apache.tinkerpop.gremlin.orientdb.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.record.impl.ODocument;

import java.util.*;

import org.apache.commons.lang.NotImplementedException;

/**
 * @author Michael Pollmeier (http://michaelpollmeier.com)
 */
public class OrientElement implements Element {
    protected OIdentifiable rawElement;
    protected OrientGraph graph;
    protected OrientElement element = this;

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
        //TODO return the property
        return null;
    }

    public void remove() {
        throw new NotImplementedException();
    }

    public <V> Iterator<? extends Property<V>> properties(final String... propertyKeys) {
        ODocument raw = rawElement.getRecord();
        Map<String, Object> properties = raw.toMap();

        //TODO: filter by propertyKeys ;)

        return new Iterator<Property<V>>() {
            Iterator<Map.Entry<String, Object>> itty = properties.entrySet().iterator();

            @Override
            public boolean hasNext() {
                return itty.hasNext();
            }

            @Override
            public Property<V> next() {
                Map.Entry<String, Object> entry = itty.next();

                return new Property<V>() {
                    @Override
                    public String key() {
                        return entry.getKey();
                    }

                    @Override
                    public V value() throws NoSuchElementException {
                        return (V) entry.getValue();
                    }

                    @Override
                    public boolean isPresent() {
                        return true;
                    }

                    @Override
                    public Element element() {
                        return element;
                    }

                    @Override
                    public void remove() {
                        throw new NotImplementedException();
                    }
                };
            }
        };
    }

}
