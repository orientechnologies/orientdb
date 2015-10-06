package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;

import java.util.Optional;

public class OrientIndexQuery {
    public final String className;
    public final String key;
    public final Object value;

    public OrientIndexQuery(boolean elementIsVertex, Optional<String> elementLabel, String key, Object value) {
        String classNamePrefix = elementIsVertex ?
            OImmutableClass.VERTEX_CLASS_NAME + "_" :
            OImmutableClass.EDGE_CLASS_NAME + "_";

        this.className = classNamePrefix + elementLabel.orElse("");
        this.key = key;
        this.value = value;
    }

    public String indexName() {
        return className + "." + key;
    }

    public String toString() {
        return "OrientIndexQuery(className=" + className + ", key=" + key + ", value=" + value + ")";
    }
}
