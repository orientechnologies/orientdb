package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OVertex;

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
      ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getLocalURL());
      if(graph.exists()){
        graph.open("admin", "admin");
      }else{
        graph.create();
      }
      graph.begin();

      try {
        // Create 2 parent vertices.
        OVertex parentV1 = graph.newVertex("vertextype1");
        parentV1.save();
        graph.commit();
        graph.begin();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getIdentity();

        OVertex parentV2 = graph.newVertex("vertextype2");
        parentV2.save();
        graph.commit();
        graph.begin();
        assertEquals(1, parentV2.getRecord().getVersion());
        parentV2Id = parentV2.getIdentity();

        // Create vertices.
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;
//          sleep(500);
          OVertex vertex = graph.newVertex("vertextype3");
          vertex.save();
          graph.commit();
          graph.begin();
          assertEquals(1, vertex.getRecord().getVersion());

          vertex.setProperty("num", i);
          vertex.save();
          graph.commit();
          graph.begin();
          assertEquals(2, vertex.getRecord().getVersion());

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV1.addEdge(vertex, "edgetype1");
              graph.commit();
              graph.begin();
              assertNotNull(parentV1.getProperty("cnt"));
              boolean edge1Exists = false;
              for (OEdge e : parentV1.getEdges(ODirection.OUT, "edgetype1")) {
                if (e.getVertex(ODirection.IN).equals(vertex)) {
                  edge1Exists = true;
                  break;
                }
              }
              assertTrue(edge1Exists);
              boolean edge2Exists = false;
              for (OEdge e : vertex.getEdges(ODirection.IN, "edgetype1")) {
                if (e.getVertex(ODirection.OUT).equals(parentV1)) {
                  edge2Exists = true;
                  break;
                }
              }
              assertTrue(edge2Exists);
              assertNotNull(vertex.getProperty("num"));
              break;
            } catch (OConcurrentModificationException c) {
              System.out.println("***********ROLLBACK***************");
              graph.rollback();
              parentV1.reload();
              vertex.reload();
            }
          }

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV2.addEdge(vertex, "edgetype2");
              graph.commit();
              graph.begin();
              assertNotNull(parentV2.getProperty("cnt"));
              boolean edge1Exists = false;
              for (OEdge e : parentV2.getEdges(ODirection.OUT, "edgetype2")) {
                if (e.getVertex(ODirection.IN).equals(vertex)) {
                  edge1Exists = true;
                  break;
                }
              }
              assertTrue(edge1Exists);
              boolean edge2Exists = false;
              for (OEdge e : vertex.getEdges(ODirection.IN, "edgetype2")) {
                if (e.getVertex(ODirection.OUT).equals(parentV2)) {
                  edge2Exists = true;
                  break;
                }
              }
              assertTrue(edge2Exists);
              assertNotNull(vertex.getProperty("num"));
              break;
            } catch (OConcurrentModificationException c) {
              System.out.println("***********ROLLBACK***************");
              graph.rollback();
              parentV2.reload();
              vertex.reload();
            }
          }
        }
      } catch (Throwable e) {
        if (exceptionInThread == null) {
          exceptionInThread = e;
        }
      } finally {
        System.out.println("Shutting down");
        graph.commit();
        graph.close();
        LOCK.notifyAll();
      }
    }
  }

  protected void dbClient2() {
    synchronized (LOCK) {
      ODatabaseDocumentTx graph = new ODatabaseDocumentTx(getRemoteURL());
      if(graph.exists()){
        graph.open("admin", "admin");
      }else{
        graph.create();
      }
      graph.begin();
      OVertex parentV1 = null;
      OVertex parentV2 = null;
      int countPropValue = 0;
      try {
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;
          // Let's give it some time for asynchronous replication.
//          sleep(500);
          countPropValue++;
          if (parentV1 == null) {
            parentV1 = graph.load((ORID)parentV1Id);
          }
          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV1.setProperty("cnt", countPropValue);
              parentV1.save();
              graph.commit();
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              parentV1.reload();
            }
          }

          if (parentV2 == null) {
            parentV2 = graph.load((ORID)parentV1Id);
          }
          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV2.setProperty("cnt", countPropValue);
              parentV2.save();
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
        graph.close();
        LOCK.notifyAll();
      }
    }
  }

}
