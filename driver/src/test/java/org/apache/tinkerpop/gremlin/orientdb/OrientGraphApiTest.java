package org.apache.tinkerpop.gremlin.orientdb;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Created by Enrico Risa on 14/06/2017.
 */
public class OrientGraphApiTest {

    @Test
    public void shouldGetEmptyEdges() {
        OrientGraph graph = OrientGraph.open();

        Vertex vertex = graph.addVertex(T.label, "Person", "name", "Foo");

        Iterator<Edge> edges = vertex.edges(Direction.OUT, "HasFriend");

        List<Edge> collected = StreamUtils.asStream(edges).collect(Collectors.toList());

        Assert.assertEquals(0, collected.size());

    }
}
