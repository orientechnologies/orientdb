package org.apache.tinkerpop.gremlin.orientdb.structure;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import org.apache.tinkerpop.gremlin.structure.*;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;

public class OrientEdge extends OrientElement implements Edge {
    public OrientEdge(OrientGraph graph, OIdentifiable rawElement) {
        super(graph, rawElement);
    }

    public OrientEdge(OrientGraph graph, String className) {
        super(graph, className);
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        throw new NotImplementedException();
    }

    public <V> Iterator<Property<V>> properties(final String... propertyKeys) {
        throw new NotImplementedException();
//        Iterator<? extends Property<V>> properties = super.properties(propertyKeys);
//        return StreamUtils.asStream(properties).map(p ->
//                        (Property<V>) new OrientProperty<>( p.key(), p.value(), (Element) p.element())
//        ).iterator();
    }
}
