package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OImmutableClass;

public class OrientEdgeType extends OrientElementType {
    // Keeping the name in Immutable class because i cannot do the other way
    // around
    public static final String CLASS_NAME = OImmutableClass.EDGE_CLASS_NAME;

    public OrientEdgeType(final OrientGraph graph, final OClass delegate) {
        super(graph, delegate);
    }

    public OrientEdgeType(final OrientGraph graph) {
        super(graph, graph.getRawDatabase().getMetadata().getSchema().getClass(CLASS_NAME));
    }

    protected static final void checkType(final OClass iType) {
        if (iType == null)
            throw new IllegalArgumentException("Edge class is null");

        if (((iType instanceof OImmutableClass) && !((OImmutableClass) iType).isEdgeType()) || !iType.isSubClassOf(CLASS_NAME))
            throw new IllegalArgumentException("Type error. The class " + iType + " does not extend class '" + CLASS_NAME
                    + "' and therefore cannot be considered an Edge");
    }

    @Override
    public OrientEdgeType getSuperClass() {
        return new OrientEdgeType(graph, super.getSuperClass());
    }

    @Override
    public OrientEdgeType addCluster(final String iClusterName) {
        delegate.addCluster(iClusterName);
        return this;
    }

    @Override
    protected String getTypeName() {
        return "edge";
    }
}
