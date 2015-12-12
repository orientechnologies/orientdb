package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClassAbstractDelegate;

public abstract class OrientElementType extends OClassAbstractDelegate {
    protected final OrientGraph graph;

    public OrientElementType(final OrientGraph graph, final OClass delegate) {
        super(delegate);
        this.graph = graph;
    }

    //    @Override
    //    public OProperty createProperty(final String iPropertyName, final OType iType, final OClass iLinkedClass) {
    //        return graph.executeOutsideTx(new OCallable<OProperty, OrientGraph>() {
    //                                          @Override
    //                                          public OProperty call(final OrientGraph g) {
    //                                              return OrientElementType.super.createProperty(iPropertyName, iType, iLinkedClass);
    //                                          }
    //                                      }, "create ", getTypeName(), " property '", iPropertyName, "' as type '", iType.toString(), "' linked class '",
    //                iLinkedClass.getName(), "'");
    //    }
    //
    //    @Override
    //    public OProperty createProperty(final String iPropertyName, final OType iType, final OType iLinkedType) {
    //        return graph.executeOutsideTx(new OCallable<OProperty, OrientGraph>() {
    //                                          @Override
    //                                          public OProperty call(final OrientGraph g) {
    //                                              return OrientElementType.super.createProperty(iPropertyName, iType, iLinkedType);
    //                                          }
    //                                      }, "create ", getTypeName(), " property '", iPropertyName, "' as type '", iType.toString(), "' linked type '",
    //                iLinkedType.toString(), "'");
    //    }
    //
    //    @Override
    //    public OProperty createProperty(final String iPropertyName, final OType iType) {
    //        return graph.executeOutsideTx(new OCallable<OProperty, OrientGraph>() {
    //            @Override
    //            public OProperty call(final OrientGraph g) {
    //                return OrientElementType.super.createProperty(iPropertyName, iType);
    //            }
    //        }, "create ", getTypeName(), " property '", iPropertyName, "' as type '", iType.toString(), "'");
    //    }
    //
    //    @Override
    //    public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final String... fields) {
    //        return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientGraph>() {
    //            @Override
    //            public OIndex<?> call(final OrientGraph g) {
    //                return OrientElementType.super.createIndex(iName, iType, fields);
    //            }
    //        }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
    //    }
    //
    //    @Override
    //    public OIndex<?> createIndex(final String iName, final String iType, final String... fields) {
    //        return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientGraph>() {
    //            @Override
    //            public OIndex<?> call(final OrientGraph g) {
    //                return OrientElementType.super.createIndex(iName, iType, fields);
    //            }
    //        }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
    //    }
    //
    //    @Override
    //    public OIndex<?> createIndex(final String iName, final INDEX_TYPE iType, final OProgressListener iProgressListener,
    //                                 final String... fields) {
    //        return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientGraph>() {
    //            @Override
    //            public OIndex<?> call(final OrientGraph g) {
    //                return OrientElementType.super.createIndex(iName, iType, iProgressListener, fields);
    //            }
    //        }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
    //    }
    //
    //    @Override
    //    public OIndex<?> createIndex(final String iName, final String iType, final OProgressListener iProgressListener,
    //                                 final ODocument metadata, final String algorithm, final String... fields) {
    //        return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientGraph>() {
    //            @Override
    //            public OIndex<?> call(final OrientGraph g) {
    //                return OrientElementType.super.createIndex(iName, iType, iProgressListener, metadata, algorithm, fields);
    //            }
    //        }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
    //    }
    //
    //    @Override
    //    public OIndex<?> createIndex(final String iName, final String iType, final OProgressListener iProgressListener,
    //                                 final ODocument metadata, final String... fields) {
    //        return graph.executeOutsideTx(new OCallable<OIndex<?>, OrientGraph>() {
    //            @Override
    //            public OIndex<?> call(final OrientGraph g) {
    //                return OrientElementType.super.createIndex(iName, iType, iProgressListener, metadata, fields);
    //            }
    //        }, "create index '", iName, "' as type '", iType.toString(), "' on fields: " + Arrays.toString(fields));
    //    }

    protected abstract String getTypeName();
}
