package org.apache.tinkerpop.gremlin.orientdb;

import static java.util.stream.Collectors.toMap;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.COMMIT;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.ROLLBACK;
import static org.apache.tinkerpop.gremlin.structure.VertexProperty.Cardinality.single;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.Iterator;
import java.util.Map;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Test;

public class OrientGraphTest {

    protected OrientGraphFactory graphFactory() {
        return new OrientGraphFactory("memory:tinkerpop-" +  Math.random());
    }

    public static final String TEST_VALUE = "SomeValue";

    @Test
    public void testGraphTransactions() throws Exception {
        OrientGraphFactory graphFactory = graphFactory();
        Object id;
        try (Graph graph = graphFactory.getTx()) {
            try (Transaction tx = graph.tx()) {
                tx.onClose(COMMIT);
                Vertex vertex = graph.addVertex();
                id = vertex.id();
                vertex.property("test", TEST_VALUE);
            }
        }
        assertNotNull("A vertex should have been created in the first transaction", id);

        // Modify property and rollback
        try (Graph graph = graphFactory.getTx()) {
            try (Transaction tx = graph.tx()) {
                tx.onClose(ROLLBACK);
                Vertex vertex = graph.vertices(id).next();
                assertNotNull(vertex);
                vertex.property("test", "changed");
            }

            try (Transaction tx = graph.tx()) {
                tx.onClose(ROLLBACK);
                Vertex vertex = graph.vertices(id).next();
                assertNotNull(vertex);
                vertex.property("test", "changed");
            }
        }

        try (Graph graph = graphFactory.getTx()) {
            Vertex vertex = graph.vertices(id).next();
            assertEquals("The property value should not have been changed.", TEST_VALUE, vertex.value("test"));
        }

        // 1. Modify property rollback, 2. Modify property commit
        try (Graph graph = graphFactory.getTx()) {
            try (Transaction tx = graph.tx()) {
                tx.onClose(ROLLBACK);
                Vertex vertex = graph.vertices(id).next();
                assertNotNull(vertex);
                vertex.property("test", "changed");
            }

            try (Transaction tx = graph.tx()) {
                tx.onClose(COMMIT);
                Vertex vertex = graph.vertices(id).next();
                assertNotNull(vertex);
                vertex.property("test", "changed");
            }
        }

        try (Graph graph = graphFactory.getTx()) {
            Vertex vertex = graph.vertices(id).next();
            assertEquals("The property value should not have been changed.", "changed", vertex.value("test"));
        }

    }

    @Test
    public void testGraphTransactionOnNoTrxOrientGraph() throws Exception {
        OrientGraphFactory graphFactory = graphFactory();
        Object id;
        try (Graph graph = graphFactory.getTx()) {
            try (Transaction tx = graph.tx()) {
                tx.onClose(COMMIT);
                Vertex vertex = graph.addVertex();
                id = vertex.id();
                vertex.property("test", TEST_VALUE);
            }
        }
        assertNotNull("A vertex should have been created in the first transaction", id);

        try (Graph graph = graphFactory.getNoTx()) {
            try (Transaction tx = graph.tx()) {
                tx.onClose(ROLLBACK);
                Vertex vertex = graph.vertices(id).next();
                assertNotNull(vertex);
                vertex.property("test", "changed");
            }
        }

        try (Graph graph = graphFactory.getTx()) {
            Vertex vertex = graph.vertices(id).next();
            assertEquals("The property value should have been changed since the graph was used in noTx mode and all actions are atomic.", "changed", vertex.value("test"));
        }
    }

    @Test
    public void testGraph() throws Exception {
        try (Graph graph = graphFactory().getTx()) {

            Vertex vertex = graph.addVertex();
            assertNotNull(vertex);
            assertNotNull(vertex.id());

            vertex.property("test", TEST_VALUE);
            assertEquals(TEST_VALUE, vertex.value("test"));

            Property<String> property = vertex.property("test");
            assertNotNull(property);
            assertTrue(property.isPresent());
            assertEquals(TEST_VALUE, property.value());
            property.remove();
            assertFalse(property.isPresent());

            // Create test vertices for edge
            Vertex vertexA = graph.addVertex();
            Vertex vertexB = graph.addVertex();
            Edge edge = vertexA.addEdge("EDGE_LABEL", vertexB);
            assertEquals("EDGE_LABEL", edge.label());

            // Test edge properties
            assertNotNull(edge.property("test", TEST_VALUE));
            Property<String> edgeProperty = edge.property("test");
            assertNotNull(edgeProperty);
            assertTrue(edgeProperty.isPresent());
            assertEquals(TEST_VALUE, edgeProperty.value());
            edgeProperty.remove();
            assertFalse(edgeProperty.isPresent());

            edge.property("test", TEST_VALUE);
            assertEquals(TEST_VALUE, edge.value("test"));

            // Check vertices of edge
            Vertex out = edge.outVertex();
            assertNotNull(out);
            assertEquals(vertexA.id(), out.id());

            Vertex in = edge.inVertex();
            assertNotNull(in);
            assertEquals(vertexB.id(), in.id());
            graph.close();
        }
    }

    @Test
    public void testStaticIterator() throws Exception {
        try (Graph graph = graphFactory().getTx()) {
            Vertex v1 = graph.addVertex();

            Iterator<Vertex> iterator = graph.vertices();

            // v2 should not be returned by the Iterator
            Vertex v2 = graph.addVertex();

            assertTrue(iterator.hasNext());
            iterator.next();
            assertFalse(iterator.hasNext());

            graph.close();
        }
    }

    @Test
    public void testMetaProperties() throws Exception {
        try (Graph graph = graphFactory().getTx()) {
            Vertex v1 = graph.addVertex();
            VertexProperty<String> prop = v1.property(single, "key", "value", "meta_key", "meta_value", "meta_key_2", "meta_value_2");

            Map<String, String> keysValues = StreamUtils.asStream(prop.properties())
                    .collect(toMap(p -> p.key(), p -> (String) p.value()));
            assertThat(keysValues, hasEntry("meta_key", "meta_value"));
            assertThat(keysValues, hasEntry("meta_key_2", "meta_value_2"));

            Map<String, Property<?>> props = StreamUtils.asStream(prop.properties())
                    .collect(toMap(p -> p.key(), p -> p));

            props.get("meta_key_2").remove();

            keysValues = StreamUtils.asStream(prop.properties())
                    .collect(toMap(p -> p.key(), p -> (String) p.value()));
            assertThat(keysValues, hasEntry("meta_key", "meta_value"));
            assertThat(keysValues, not(hasEntry("meta_key_2", "meta_value_2")));

            props.get("meta_key").remove();

            keysValues = StreamUtils.asStream(prop.properties())
                    .collect(toMap(p -> p.key(), p -> (String) p.value()));
            assertThat(keysValues, not(hasEntry("meta_key", "meta_value")));
            assertThat(keysValues, not(hasEntry("meta_key_2", "meta_value_2")));

            graph.close();
        }
    }

}
