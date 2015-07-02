package com.orientechnologies.orient.server.distributed.asynch;

import java.util.Iterator;

import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestGetVertexEdgesNoReload extends BareBoneBase2ClientTest {

  private Object vertex1Id;
  private Object vertex2Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode";
  }

  protected void dbClient1() {
    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getLocalURL());

      try {
        OrientVertex vertex1 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, vertex1.getRecord().getVersion());
        vertex1Id = vertex1.getId();

        OrientVertex vertex2 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, vertex2.getRecord().getVersion());
        vertex2Id = vertex2.getId();

        OrientVertex vertex3 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, vertex3.getRecord().getVersion());

        vertex1.addEdge("edgetype", vertex3);
        graph.commit();
        assertEquals(2, vertex1.getRecord().getVersion());
        assertEquals(2, vertex3.getRecord().getVersion());

        pause();

        int vertex1EdgeCount = 0;
        //vertex1.reload();
        for (Iterator<Edge> i = vertex1.getEdges(Direction.OUT, "edgetype").iterator(); i.hasNext(); i.next()) {
          vertex1EdgeCount ++;
        }
        assertEquals(2, vertex1EdgeCount);
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

  protected void dbClient2() {
    sleep(500);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getRemoteURL());

      try {
        OrientVertex vertex1 = null;
        OrientVertex vertex2 = null;

        vertex1 = graph.getVertex(vertex1Id);
        vertex2 = graph.getVertex(vertex2Id);

        vertex1.addEdge("edgetype", vertex2);
        graph.commit();
        assertEquals(3, vertex1.getRecord().getVersion());
        assertEquals(2, vertex2.getRecord().getVersion());

        pause();
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

}
