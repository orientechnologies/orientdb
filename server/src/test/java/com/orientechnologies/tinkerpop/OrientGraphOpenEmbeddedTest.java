package com.orientechnologies.tinkerpop;

import org.apache.tinkerpop.gremlin.orientdb.OrientGraph;
import org.apache.tinkerpop.gremlin.orientdb.OrientGraphFactory;
import org.apache.tinkerpop.gremlin.orientdb.executor.OGremlinResultSet;
import org.junit.Assert;
import org.junit.Test;

public class OrientGraphOpenEmbeddedTest extends AbstractRemoteTest {

  @Test
  public void openFactoryEmbeddedTest() {
    OrientGraphFactory factory =
        new OrientGraphFactory("embedded:target/databases/" + name.getMethodName());
    OrientGraph graph = factory.getNoTx();
    try {
      OGremlinResultSet resultSet = graph.executeSql("select from OUser");
      Assert.assertEquals(1, resultSet.stream().count());
    } finally {
      graph.close();
      factory.drop();
      factory.close();
    }
  }
}
