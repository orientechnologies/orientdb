package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.common.log.OLogManager;
import com.tinkerpop.blueprints.Vertex;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestReplicationVersionIncrementedByOne extends BareBoneBase1ClientTest {

  @Override
  protected String getDatabaseName() {
    return "TestReplicationVersionIncrementedByOne";
  }

  protected void dbClient1() {
    OrientBaseGraph graph = new OrientGraph(getLocalURL());
    try {
      Vertex v1 = graph.addVertex("vertextype", (String) null);
      graph.commit();
      assertEquals(1, ((OrientVertex) v1).getRecord().getVersion());

      Vertex v2 = graph.addVertex("vertextype", (String) null);
      graph.commit();
      assertEquals(1, ((OrientVertex) v2).getRecord().getVersion());

      v1.addEdge("edgetype", v2);
      graph.commit();
      assertEquals(2, ((OrientVertex) v1).getRecord().getVersion());
      assertEquals(2, ((OrientVertex) v2).getRecord().getVersion());
    } catch (Throwable e) {
      if (exceptionInThread == null) {
        exceptionInThread = e;
      }
    } finally {
      OLogManager.instance().info(this, "Shutting down");
      graph.shutdown();
    }
  }

}
