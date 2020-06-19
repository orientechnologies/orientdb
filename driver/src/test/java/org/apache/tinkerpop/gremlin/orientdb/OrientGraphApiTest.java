package org.apache.tinkerpop.gremlin.orientdb;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 14/06/2017. */
public class OrientGraphApiTest {

  @Test
  public void shouldGetEmptyEdges() {
    OrientGraph graph = OrientGraph.open();

    Vertex vertex = graph.addVertex(T.label, "Person", "name", "Foo");

    Iterator<Edge> edges = vertex.edges(Direction.OUT, "HasFriend");

    List<Edge> collected = StreamUtils.asStream(edges).collect(Collectors.toList());

    Assert.assertEquals(0, collected.size());
  }

  @Test
  public void testLinklistProperty() {
    OrientGraph graph = OrientGraph.open();

    Vertex vertex = graph.addVertex(T.label, "Person", "name", "Foo");
    Vertex vertex2 = graph.addVertex(T.label, "Person", "name", "Bar");
    Vertex vertex3 = graph.addVertex(T.label, "Person", "name", "Baz");

    List listProp = new ArrayList();
    listProp.add(vertex2.id());
    listProp.add(vertex3.id());

    vertex.property("links", listProp);

    Object retrieved = vertex.value("links");
    Assert.assertTrue(retrieved instanceof List);
    List resultList = (List) retrieved;
    for (Object o : resultList) {
      Assert.assertTrue(o instanceof Vertex);
    }

    graph.close();
  }
}
