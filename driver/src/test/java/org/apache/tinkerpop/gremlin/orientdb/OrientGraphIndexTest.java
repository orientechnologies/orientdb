package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.index.OIndex;
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

    String label1 = "SomeVertexLabel1";
    String label2 = "SomeVertexLabel2";
    String key = "indexedKey";

    @Test
    public void vertexUniqueConstraint() {
        OrientGraph graph = newGraph();
        createVertexIndexLabel1(graph);
        String value = "value1";

        graph.addVertex(T.label, label1, key, value);
        graph.addVertex(T.label, label2, key, value);

        // no duplicates allowed for vertex with label1
        try {
            graph.addVertex(T.label, label1, key, value);
            Assert.fail("must throw duplicate key here!");
        } catch (ORecordDuplicatedException e) {
            // ok
        }

        // allow duplicate for vertex with label2
        graph.addVertex(T.label, label2, key, value);
    }

    @Test
    public void vertexIndexLookupWithValue() {
        OrientGraph graph = newGraph();
        createVertexIndexLabel1(graph);
        String value = "value1";

        // verify index created
        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, label1), new HashSet<>(Collections.singletonList(key)));
        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, label2), new HashSet<>(Collections.emptyList()));
        Assert.assertEquals(graph.getIndexedKeys(Edge.class, label1), new HashSet<>(Collections.emptyList()));

        Vertex v1 = graph.addVertex(T.label, label1, key, value);
        Vertex v2 = graph.addVertex(T.label, label2, key, value);

        // looking deep into the internals here - I can't find a nicer way to auto verify that an index is actually used
        GraphTraversal<Vertex, Vertex> traversal = graph.traversal().V().has(T.label, P.eq(label1)).has(key, P.eq(value));
        OrientGraphStepStrategy.instance().apply(traversal.asAdmin());

        OrientGraphStep orientGraphStep = (OrientGraphStep) traversal.asAdmin().getStartStep();
        OrientIndexQuery orientIndexQuery = (OrientIndexQuery) orientGraphStep.findIndex().get();

        OIndex index = orientIndexQuery.index;
        Assert.assertEquals(1, index.getSize());
        Assert.assertEquals(v1.id(), index.get(value));
    }

    // Indexed edge properties is not yet handled / implemented.
    //    @Test
    //    public void uniqueIndexOnEdges() {
    //
    //        String vertexLabel = "SomeVertexLabel";
    //        String edgeLabel1 = "SomeEdgeLabel1";
    //        String edgeLabel2 = "SomeEdgeLabel2";
    //        String indexedKey = "indexedKey";
    //        String value = "value1";
    //
    //        OrientGraph graph = newGraph();
    //
    //        Configuration config = new BaseConfiguration();
    //        config.setProperty("type", "UNIQUE");
    //        config.setProperty("keytype", OType.STRING);
    //        graph.createEdgeIndex(indexedKey, edgeLabel1, config);
    //
    //        Assert.assertEquals(graph.getIndexedKeys(Edge.class, edgeLabel1), new HashSet<String>(Collections.singletonList(indexedKey)));
    //        Assert.assertEquals(graph.getIndexedKeys(Edge.class, edgeLabel2), new HashSet<String>(Collections.emptyList()));
    //        Assert.assertEquals(graph.getIndexedKeys(Vertex.class, vertexLabel), new HashSet<String>(Collections.emptyList()));
    //
    //        Vertex v1 = graph.addVertex(T.label, vertexLabel);
    //        Vertex v2 = graph.addVertex(T.label, vertexLabel);
    //        Edge e1 = v1.addEdge(edgeLabel1, v2, indexedKey, value);
    //        Edge e2 = v1.addEdge(edgeLabel2, v2, indexedKey, value);
    //
    //        // Verify that the traversal hits the index via println debugging.
    //        // Should print "index will be queried..." then "not indexed"
    //        Set<Edge> result1 = graph.traversal().E().has(T.label, P.eq(edgeLabel1)).toSet();
    //        Assert.assertTrue(result1.size() == 1);
    //        Set<Edge> result2 = graph.traversal().E().has(T.label, P.eq(edgeLabel2)).toSet();
    //        Assert.assertTrue(result2.size() == 1);
    //
    //        // no duplicates allowed for edge with label1
    //        try {
    //            v2.addEdge(edgeLabel1, v1, indexedKey, value);
    //            Assert.fail("must throw duplicate key here!");
    //        } catch (ORecordDuplicatedException e) {
    //            // ok
    //        }
    //
    //        // allow duplicate for vertex with label2
    //        v2.addEdge(edgeLabel2, v1, indexedKey, value);
    //    }

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

    private void createVertexIndexLabel1(OrientGraph graph) {
        Configuration config = new BaseConfiguration();
        config.setProperty("type", "UNIQUE");
        config.setProperty("keytype", OType.STRING);
        graph.createVertexIndex(key, label1, config);
    }
}
