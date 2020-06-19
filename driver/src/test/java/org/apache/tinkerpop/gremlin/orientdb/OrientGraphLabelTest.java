package org.apache.tinkerpop.gremlin.orientdb;

import java.util.Date;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 14/11/16. */
public class OrientGraphLabelTest extends OrientGraphBaseTest {

  @Test
  public void testNewLabel() {

    OrientGraph noTx = factory.getNoTx();

    Vertex vertex = noTx.addVertex(T.label, "Person", "name", "John");
    Vertex vertex1 = noTx.addVertex(T.label, "Person", "name", "Luke");

    vertex.addEdge("Friend", vertex1, "from", new Date());

    Assert.assertEquals(2, noTx.getRawDatabase().countClass("Person"));
    Assert.assertEquals(1, noTx.getRawDatabase().countClass("Friend"));
  }

  //  @Test
  public void testOldLabel() {

    OrientGraph noTx = factory.getNoTx();

    Vertex vertex = noTx.addVertex(T.label, "Person", "name", "John");
    Vertex vertex1 = noTx.addVertex(T.label, "Person", "name", "Luke");

    vertex.addEdge("Friend", vertex1, "from", new Date());

    Assert.assertEquals(2, noTx.getRawDatabase().countClass("V_Person"));
    Assert.assertEquals(1, noTx.getRawDatabase().countClass("E_Friend"));
  }
}
