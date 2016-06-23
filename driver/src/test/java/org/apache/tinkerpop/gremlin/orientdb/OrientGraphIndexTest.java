package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.orientdb.traversal.step.sideEffect.OrientGraphStep;
import org.apache.tinkerpop.gremlin.orientdb.traversal.strategy.optimization.OrientGraphStepStrategy;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class OrientGraphIndexTest {

    public static final String URL = "memory:" + OrientGraphIndexTest.class.getSimpleName();
    //    public static final String URL = "remote:localhost/test";

    private OrientGraph newGraph() {
        return new OrientGraphFactory(URL + UUID.randomUUID(), "root", "root").getNoTx();
    }

    String vertexLabel1 = "SomeVertexLabel1";
    String vertexLabel2 = "SomeVertexLabel2";

    String edgeLabel1 = "SomeEdgeLabel1";
    String edgeLabel2 = "SomeEdgeLabel2";

    String key = "indexedKey";

    @Test
    public void vertexUniqueConstraint() {
        OrientGraph graph = newGraph();
        createVertexIndexLabel1(graph);
        String value = "value1";

        graph.addVertex(T.label, vertexLabel1, key, value);
        graph.addVertex(T.label, vertexLabel2, key, value);

        // no duplicates allowed for vertex with label1
        try {
            graph.addVertex(T.label, vertexLabel1, key, value);
            Assert.fail("must throw duplicate key here!");
        } catch (ORecordDuplicatedException e) {
            // ok
        }

        // allow duplicate for vertex with label2
        graph.addVertex(T.label, vertexLabel2, key, value);
    }

    @Test
    public void edgeUniqueConstraint() {
        OrientGraph graph = newGraph();
        createUniqueEdgeIndex(graph, edgeLabel1);
        String value = "value1";

        Vertex v1 = graph.addVertex(T.label, vertexLabel1);
        Vertex v2 = graph.addVertex(T.label, vertexLabel1);
        v1.addEdge(edgeLabel1, v2, key, value);

        // no duplicates allowed for edge with label1
        try {
            v1.addEdge(edgeLabel1, v2, key, value);
            Assert.fail("must throw duplicate key here!");
        } catch (ORecordDuplicatedException e) {
            // ok
        }

        // allow duplicate for vertex with label2
        v2.addEdge(edgeLabel2, v1, key, value);
    }

    @Test
    public void vertexUniqueIndexLookupWithValue() {
        OrientGraph graph = newGraph();
        createVertexIndexLabel1(graph);
        String value = "value1";

        // verify index created
        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, vertexLabel1), new HashSet<>(Collections.singletonList(key)));
        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, vertexLabel2), new HashSet<>(Collections.emptyList()));
        Assert.assertEquals(graph.getIndexedKeys(Edge.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

        Vertex v1 = graph.addVertex(T.label, vertexLabel1, key, value);
        Vertex v2 = graph.addVertex(T.label, vertexLabel2, key, value);

        // looking deep into the internals here - I can't find a nicer way to
        // auto verify that an index is actually used
        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(T.label, P.eq(vertexLabel1)).has(key, P.eq(value));

        OIndex index = findUsedIndex(traversal).get().index;
        Assert.assertEquals(1, index.getSize());
        Assert.assertEquals(v1.id(), index.get(value));

        List<Vertex> result = traversal.toList();
        Assert.assertEquals(1, result.size());
        Assert.assertEquals(v1.id(), result.get(0).id());
    }

    @Test
    public void edgeUniqueIndexLookupWithValue() {
        OrientGraph graph = newGraph();
        createUniqueEdgeIndex(graph, edgeLabel1);
        String value = "value1";

        Assert.assertEquals(graph.getIndexedKeys(Edge.class, edgeLabel1), new HashSet<>(Collections.singletonList(key)));
        Assert.assertEquals(graph.getIndexedKeys(Edge.class, edgeLabel2), new HashSet<>(Collections.emptyList()));
        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

        Vertex v1 = graph.addVertex(T.label, vertexLabel1);
        Vertex v2 = graph.addVertex(T.label, vertexLabel1);
        Edge e1 = v1.addEdge(edgeLabel1, v2, key, value);
        Edge e2 = v1.addEdge(edgeLabel2, v2, key, value);

        {
            // Verify that the traversal hits the index for the edges with label1
            GraphTraversal<Edge, Edge> traversal1 = graph.traversal().E().has(T.label, P.eq(edgeLabel1)).has(key, P.eq(value));
            Optional<OrientIndexQuery> orientIndexQuery = findUsedIndex(traversal1);
            Assert.assertTrue(orientIndexQuery.isPresent());

            OIndex index = orientIndexQuery.get().index;
            Assert.assertEquals(1, index.getSize());
            Assert.assertEquals(e1.id(), index.get(value));

            List<Edge> result1 = traversal1.toList();
            Assert.assertEquals(1, result1.size());
            Assert.assertEquals(e1.id(), result1.get(0).id());
        }

        {
            // Verify that the traversal doesn't try to hit the index for the edges with label2
            GraphTraversal<Edge, Edge> traversal2 = graph.traversal().E().has(T.label, P.eq(edgeLabel2)).has(key, P.eq(value));
            Assert.assertFalse(findUsedIndex(traversal2).isPresent());

            List<Edge> result2 = traversal2.toList();
            Assert.assertEquals(1, result2.size());
            Assert.assertEquals(e2.id(), result2.get(0).id());
        }
    }

    @Test
    public void edgeNotUniqueIndexLookupWithValue() {
        OrientGraph graph = newGraph();

        createNotUniqueEdgeIndex(graph, edgeLabel1);

        String value = "value1";

        Assert.assertEquals(graph.getIndexedKeys(Edge.class, edgeLabel1), new HashSet<>(Collections.singletonList(key)));
        Assert.assertEquals(graph.getIndexedKeys(Edge.class, edgeLabel2), new HashSet<>(Collections.emptyList()));
        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, vertexLabel1), new HashSet<>(Collections.emptyList()));

        Vertex v1 = graph.addVertex(T.label, vertexLabel1);
        Vertex v2 = graph.addVertex(T.label, vertexLabel1);
        Edge e1 = v1.addEdge(edgeLabel1, v2, key, value);
        Edge e2 = v1.addEdge(edgeLabel1, v2, key, value);
        Edge e3 = v1.addEdge(edgeLabel1, v2);

        // Verify that the traversal hits the index for the edges with label1
        GraphTraversal<Edge, Edge> traversal1 = graph.traversal().E().has(T.label, P.eq(edgeLabel1)).has(key, P.eq(value));
        Optional<OrientIndexQuery> orientIndexQuery = findUsedIndex(traversal1);
        Assert.assertTrue(orientIndexQuery.isPresent());

        OIndex index = orientIndexQuery.get().index;
        Assert.assertEquals(2, index.getSize());
        Assert.assertTrue(((Collection) index.get(value)).contains(e1.id()));
        Assert.assertTrue(((Collection) index.get(value)).contains(e2.id()));
        Assert.assertFalse(((Collection) index.get(value)).contains(e3.id()));

        List<Edge> result1 = traversal1.toList();
        Assert.assertEquals(2, result1.size());
        Assert.assertTrue(result1.stream().map(Edge::id).anyMatch(e1.id()::equals));
        Assert.assertTrue(result1.stream().map(Edge::id).anyMatch(e2.id()::equals));
        Assert.assertFalse(result1.stream().map(Edge::id).anyMatch(e3.id()::equals));
    }

    //TODO: fix
    @Test
    public void indexCollation() {
        OrientGraph graph = newGraph();

        String label = "VC1";
        String key = "name";
        String value = "bob";

        Configuration config = new BaseConfiguration();
        config.setProperty("type", "UNIQUE");
        config.setProperty("keytype", OType.STRING);
        config.setProperty("collate", "ci");
        graph.createVertexIndex(key, label, config);

        graph.addVertex(T.label, label, key, value);
        // TODO: test with a "has" traversal, if/when that supports a case insensitive match predicate
        //        OrientIndexQuery indexRef = new OrientIndexQuery(true, Optional.of(label), key, value.toUpperCase());
        //        Iterator<OrientVertex> result = graph.getIndexedVertices(indexRef).iterator();
        //        Assert.assertEquals(result.hasNext(), true);
    }

    private Optional<OrientIndexQuery> findUsedIndex(GraphTraversal<?, ?> traversal) {
        OrientGraphStepStrategy.instance().apply(traversal.asAdmin());

        @SuppressWarnings("rawtypes")
        OrientGraphStep orientGraphStep = (OrientGraphStep) traversal.asAdmin().getStartStep();

        return orientGraphStep.findIndex();
    }

    private void createVertexIndexLabel1(OrientGraph graph) {
        Configuration config = new BaseConfiguration();
        config.setProperty("type", "UNIQUE");
        config.setProperty("keytype", OType.STRING);
        graph.createVertexIndex(key, vertexLabel1, config);
    }

    private void createUniqueEdgeIndex(OrientGraph graph, String label) {
        Configuration config = new BaseConfiguration();
        config.setProperty("type", OClass.INDEX_TYPE.UNIQUE.name());
        config.setProperty("keytype", OType.STRING);
        graph.createEdgeIndex(key, label, config);
    }

    private void createNotUniqueEdgeIndex(OrientGraph graph, String label) {
        Configuration config = new BaseConfiguration();
        config.setProperty("type", OClass.INDEX_TYPE.NOTUNIQUE.name());
        config.setProperty("keytype", OType.STRING);
        graph.createEdgeIndex(key, label, config);
    }
}
