package org.apache.tinkerpop.gremlin.orientdb;

import java.util.Iterator;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Test;

public class OrientSampleTest {

  @Test
  public void labelTest() {
    String graphUri = "memory:test";
    //        String graphUri = "plocal:target/graph" + Math.random();
    //        String graphUri = "remote:localhost/test";
    OrientGraph graph = new OrientGraphFactory(graphUri, "admin", "admin").getNoTx();

    OrientVertex v1 = (OrientVertex) graph.addVertex();
    OrientVertex v2 = (OrientVertex) graph.addVertex();
    OrientEdge e = (OrientEdge) v1.addEdge("label1", v2);

    Iterator<Vertex> vertices = graph.vertices();
    while (vertices.hasNext()) System.out.println(vertices.next());

    //        OClass clazz = v1.getRawDocument().getSchemaClass();
    //        Collection<OClass> subclasses = clazz.getSubclasses();
    //        System.out.println("Subclasses of vertex: " + subclasses.size());
    //        subclasses.forEach(c -> System.out.println(c));
  }
}
