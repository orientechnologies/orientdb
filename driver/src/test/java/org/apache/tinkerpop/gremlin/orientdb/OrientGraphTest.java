package org.apache.tinkerpop.gremlin.orientdb;

import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.COMMIT;
import static org.apache.tinkerpop.gremlin.structure.Transaction.CLOSE_BEHAVIOR.ROLLBACK;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

import java.util.Iterator;

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

    @Test
    public void testGraphTransactionOnNoTrxOrientGraph() throws Exception {

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

    @Test
    public void testStaticIterator() throws Exception {
        try (Graph graph = graphFactory.getTx()) {
            Vertex v1 = graph.addVertex();

            Iterator<Vertex> iterator = graph.vertices();

            // v2 should not be returned by the Iterator
            Vertex v2 = graph.addVertex();

            assertTrue(iterator.hasNext());
            assertEquals(v1, iterator.next());
            assertFalse(iterator.hasNext());

            graph.close();
        }
    }
}
