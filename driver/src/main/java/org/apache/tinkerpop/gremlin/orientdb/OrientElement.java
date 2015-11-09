package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;
import com.orientechnologies.orient.core.record.impl.ODocument;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;


public abstract class OrientElement implements Element {

    private static final Map<String, String> INTERNAL_CLASSES_TO_TINKERPOP_CLASSES;
    static {
        INTERNAL_CLASSES_TO_TINKERPOP_CLASSES = new HashMap<>();
        INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.put(OImmutableClass.VERTEX_CLASS_NAME, Vertex.DEFAULT_LABEL);
        INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.put(OImmutableClass.EDGE_CLASS_NAME, Edge.DEFAULT_LABEL);
    }

    protected OIdentifiable rawElement;
    protected OrientGraph graph;

    public OrientElement(final OrientGraph graph, final OIdentifiable rawElement) {
        if (rawElement == null)
            throw new IllegalArgumentException("rawElement must not be null!");
        this.graph = checkNotNull(graph);
        this.rawElement = checkNotNull(rawElement);
    }

    public ORID id() {
        return rawElement.getIdentity();
    }

    public String label() {
        String internalClassName = getRawDocument().getClassName();
        // User labels on edges/vertices are prepended with E_ or V_ . The user should not see that.
        return internalClassName.length() == 1 ? INTERNAL_CLASSES_TO_TINKERPOP_CLASSES.get(internalClassName) : internalClassName.substring(2);
    }

    public Graph graph() {
        return graph;
    }

    public <V> Property<V> property(final String key, final V value) {
        if(key == null)
            throw Property.Exceptions.propertyKeyCanNotBeNull();
        if(value == null)
            throw Property.Exceptions.propertyKeyCanNotBeNull();
        if(Graph.Hidden.isHidden(key))
            throw Property.Exceptions.propertyKeyCanNotBeAHiddenKey(key);

        ODocument doc = getRawDocument();
        doc.field(key, value);
        doc.save();
        return new OrientProperty<>(key, value, this);
    }

    public void property(Object... keyValues) {
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        if (ElementHelper.getIdValue(keyValues).isPresent()) throw Vertex.Exceptions.userSuppliedIdsNotSupported();

        ElementHelper.attachProperties(this, keyValues);
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

        @SuppressWarnings("unchecked")
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

    @Override
    public final int hashCode() {
        return ElementHelper.hashCode(this);
    }

    @Override
    public final boolean equals(final Object object) {
        return ElementHelper.areEqual(this, object);
    }


}
