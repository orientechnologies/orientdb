package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.index.OIndex;
import java.util.Optional;

public class OrientIndexQuery {
    public final Optional<Object> value;
    public final OIndex index;

    public OrientIndexQuery(OIndex index, Optional<Object> value) {
        this.index = index;
        this.value = value;
    }

    public String toString() {
        return "OrientIndexQuery(index=" + index + ", value=" + value + ")";
    }
}
