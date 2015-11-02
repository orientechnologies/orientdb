package org.apache.tinkerpop.gremlin.orientdb;

import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.COMMIT;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.ROLLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class OrientGraphTest {

    protected OrientGraphFactory graphFactory = new OrientGraphFactory("memory:tinkerpop");

    public static final String TEST_VALUE = "SomeValue";

    @Test
    public void testGraphTransactions() throws Exception {
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

    @Test(expected = RuntimeException.class)
    public void testGraphTransactionOnNoTrxOrientGraph() throws Exception {
        try (Graph graph = graphFactory.getNoTx()) {
            graph.tx();
            fail("It should not be possible to access the transaction object when using a no trx graph");
        }
    }

    @Test
    public void testGraph() throws Exception {

        try (Graph graph = graphFactory.getTx()) {

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
}
