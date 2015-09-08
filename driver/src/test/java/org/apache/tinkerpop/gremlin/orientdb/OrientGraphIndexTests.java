package org.apache.tinkerpop.gremlin.orientdb;

import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class OrientGraphIndexTests {

    public static final String URL = "memory:" + OrientGraphIndexTests.class.getSimpleName();
//    public static final String URL = "remote:localhost/test";

    private OrientGraph newGraph() {
        return new OrientGraphFactory(URL + UUID.randomUUID(), "root", "root").getNoTx();
    }

    @Test
    public void uniqueIndex() {
        String className = "V";
        String keyName = "key1";
        String value = "value1";

        OrientGraph graph = newGraph();
        graph.createVertexClass(className);

        Configuration config = new BaseConfiguration();
//        config.setProperty("class", className);
        config.setProperty("type", "UNIQUE");
        config.setProperty("keytype", OType.STRING);
        graph.createIndex(keyName, Vertex.class, config);

        Assert.assertEquals(graph.getIndexedKeys(Vertex.class), new HashSet<String>(Collections.singletonList(keyName)));

        graph.addVertex(T.label, className, keyName, value);

        // This test doesn't check that the following traversal hit the index.
        // Only verified by println debugging in OrientGraphStep.
        Set<Vertex> result = graph.traversal().V().has(T.label, P.eq(className)).has(keyName, P.eq(value)).toSet();
        Assert.assertTrue(result.size() == 1);

        try {
            graph.addVertex(T.label, className, keyName, value);
            Assert.fail("must throw duplicate key here!");
        } catch (ORecordDuplicatedException e) {
            // ok
        }
    }

    @Test
    public void indexCollation() {
        String className = "VC1";
        String keyName = "name";
        String value = "bob";

        OrientGraph graph = newGraph();
        graph.createVertexClass(className);

        Configuration config = new BaseConfiguration();
        config.setProperty("class", className);
        config.setProperty("type", "UNIQUE");
        config.setProperty("keytype", OType.STRING);
        config.setProperty("collate", "ci");
        graph.createIndex(keyName, Vertex.class, config);

        graph.addVertex(T.label, className, keyName, value);
        // TODO: test with a "has" traversal, if/when that supports a case insensitive match predicate
        Iterator<OrientVertex> result = graph.getIndexedVertices(className + "." + keyName, value.toUpperCase()).iterator();
        Assert.assertEquals(result.hasNext(), true);
    }
}
