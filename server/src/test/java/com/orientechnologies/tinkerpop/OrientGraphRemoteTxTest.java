package com.orientechnologies.tinkerpop;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;

/** Created by Enrico Risa on 19/05/2017. */
public class OrientGraphRemoteTxTest extends AbstractRemoteGraphFactoryTest {

  @Override
  public void setup() throws Exception {
    super.setup();
    OrientGraph noTx = factory.getNoTx();

    noTx.executeSql("CREATE CLASS Person EXTENDS V");
    noTx.executeSql("CREATE CLASS HasFriend EXTENDS E");
    noTx.executeSql("CREATE SEQUENCE personIdSequence TYPE ORDERED;");
    noTx.executeSql(
        "CREATE PROPERTY Person.id LONG (MANDATORY TRUE, default \"sequence('personIdSequence').next()\");");
    noTx.executeSql("CREATE INDEX Person.id ON Person (id) UNIQUE");

    noTx.close();
  }

  @Test
  public void txSequenceTest() {

    OrientGraph tx = factory.getTx();
    Vertex vertex = tx.addVertex(T.label, "Person", "name", "John");
    for (int i = 0; i < 10; i++) {
      Vertex vertex1 = tx.addVertex(T.label, "Person", "name", "Frank" + i);
      vertex.addEdge("HasFriend", vertex1);
    }
    tx.commit();

    Assert.assertEquals(11, tx.getRawDatabase().countClass("Person"));
  }
}
