package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.tinkerpop.blueprints.Direction;
import com.tinkerpop.blueprints.Edge;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestAsyncReplMode extends BareBoneBase2ClientTest {

  private static final int NUM_OF_LOOP_ITERATIONS = 25;
  private static final int NUM_OF_RETRIES         = 3;

  private Object           parentV1Id;
  private Object           parentV2Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode";
  }

  protected void dbClient1() {
    sleep(1000);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getLocalURL());
      try {
        // Create 2 parent vertices.
        OrientVertex parentV1 = graph.addVertex("vertextype1", (String) null);
        graph.commit();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getId();

        OrientVertex parentV2 = graph.addVertex("vertextype2", (String) null);
        graph.commit();
        assertEquals(1, parentV2.getRecord().getVersion());
        parentV2Id = parentV2.getId();

        // Create vertices.
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;
          sleep(500);
          OrientVertex vertex = graph.addVertex("vertextype3", (String) null);
          graph.commit();
          assertEquals(1, vertex.getRecord().getVersion());

          vertex.setProperty("num", i);
          graph.commit();
          assertEquals(2, vertex.getRecord().getVersion());

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV1.addEdge("edgetype1", vertex);
              graph.commit();
              assertNotNull(parentV1.getProperty("cnt"));
              boolean edge1Exists = false;
              for (Edge e : parentV1.getEdges(Direction.OUT, "edgetype1")) {
                if (e.getVertex(Direction.IN).equals(vertex)) {
                  edge1Exists = true;
                  break;
                }
              }
              assertTrue(edge1Exists);
              boolean edge2Exists = false;
              for (Edge e : vertex.getEdges(Direction.IN, "edgetype1")) {
                if (e.getVertex(Direction.OUT).equals(parentV1)) {
                  edge2Exists = true;
                  break;
                }
              }
              assertTrue(edge2Exists);
              assertNotNull(vertex.getProperty("num"));
              break;
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              if (c.getRid().equals(parentV1.getId())) {
                parentV1.reload();
              } else {
                vertex.reload();
              }
            }
          }

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV2.addEdge("edgetype2", vertex);
              graph.commit();
              assertNotNull(parentV2.getProperty("cnt"));
              boolean edge1Exists = false;
              for (Edge e : parentV2.getEdges(Direction.OUT, "edgetype2")) {
                if (e.getVertex(Direction.IN).equals(vertex)) {
                  edge1Exists = true;
                  break;
                }
              }
              assertTrue(edge1Exists);
              boolean edge2Exists = false;
              for (Edge e : vertex.getEdges(Direction.IN, "edgetype2")) {
                if (e.getVertex(Direction.OUT).equals(parentV2)) {
                  edge2Exists = true;
                  break;
                }
              }
              assertTrue(edge2Exists);
              assertNotNull(vertex.getProperty("num"));
              break;
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              if (c.getRid().equals(parentV2.getId())) {
                parentV2.reload();
              } else {
                vertex.reload();
              }
            }
          }
        }
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

  protected void dbClient2() {
    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getRemoteURL());
      OrientVertex parentV1 = null;
      OrientVertex parentV2 = null;
      int countPropValue = 0;
      try {
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;
          // Let's give it some time for asynchronous replication.
          sleep(500);
          countPropValue++;
          if (parentV1 == null) {
            parentV1 = graph.getVertex(parentV1Id);
          }
          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV1.setProperty("cnt", countPropValue);
              graph.commit();
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              parentV1.reload();
            }
          }

          if (parentV2 == null) {
            parentV2 = graph.getVertex(parentV2Id);
          }
          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV2.setProperty("cnt", countPropValue);
              graph.commit();
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              parentV2.reload();
            }
          }
        }
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.shutdown();
        LOCK.notifyAll();
      }
    }
  }

}
