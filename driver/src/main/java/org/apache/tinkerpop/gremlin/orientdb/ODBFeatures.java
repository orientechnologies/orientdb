package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
import org.apache.tinkerpop.gremlin.structure.Graph.Features.VertexFeatures;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

public class ODBFeatures {

    public static class OrientFeatures implements Features {

        static final OrientFeatures INSTANCE = new OrientFeatures();

        private OrientFeatures() {
        }

        @Override
        public GraphFeatures graph() {
            return OrientGraphFeatures.INSTANCE;
        }

        @Override
        public EdgeFeatures edge() {
            return OrientEdgeFeatures.INSTANCE;
        }

        @Override
        public VertexFeatures vertex() {
            return OrientVertexFeatures.INSTANCE;
        }

        @Override
        public String toString() {
            return StringFactory.featureString(this);
        }
    }

    public static abstract class OrientElementFeatures implements Features.ElementFeatures {

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

        @Override
        public boolean supportsUserSuppliedIds() {
            return false;
        }

    }

    public static class OrientVertexFeatures extends OrientElementFeatures implements Features.VertexFeatures {

        static final OrientVertexFeatures INSTANCE = new OrientVertexFeatures();

        private OrientVertexFeatures() {
        }

        @Override
        public boolean supportsRemoveVertices() {
            return false;
        }

        @Override
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public boolean supportsMetaProperties() {
            return false;
        }

    }

    public static class OrientEdgeFeatures extends OrientElementFeatures implements Features.EdgeFeatures {

        static final OrientEdgeFeatures INSTANCE = new OrientEdgeFeatures();

        private OrientEdgeFeatures() {
        }

        @Override
        public boolean supportsRemoveEdges() {
            return false;
        }

    }

    public static class OrientGraphFeatures implements Features.GraphFeatures {

        static final OrientGraphFeatures INSTANCE = new OrientGraphFeatures();

        private OrientGraphFeatures() {
        }

        @Override
        public boolean supportsComputer() {
            return false;
        }

        @Override
        public boolean supportsTransactions() {
            return false;
        }

        @Override
        public boolean supportsPersistence() {
            return false;
        }

        @Override
        public boolean supportsThreadedTransactions() {
            return false;
        }

    }
}
