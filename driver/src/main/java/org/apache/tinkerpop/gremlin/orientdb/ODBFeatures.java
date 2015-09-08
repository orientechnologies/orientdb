package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.Graph.Features;
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

    public static class OrientVertexFeatures implements Features.VertexFeatures {

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

        @Override
        public boolean supportsCustomIds() {
            return false;
        }

    }

    public static class OrientEdgeFeatures implements Features.EdgeFeatures {

        static final OrientEdgeFeatures INSTANCE = new OrientEdgeFeatures();

        private OrientEdgeFeatures() {
        }

        @Override
        public boolean supportsRemoveEdges() {
            return false;
        }

        @Override
        public boolean supportsCustomIds() {
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
