package com.orientechnologies.orient.server.distributed.asynch;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.exception.OConcurrentModificationException;
import com.tinkerpop.blueprints.impls.orient.OrientBaseGraph;
import com.tinkerpop.blueprints.impls.orient.OrientGraph;
import com.tinkerpop.blueprints.impls.orient.OrientVertex;

public class TestAsyncReplMode2Servers extends BareBoneBase2ServerTest {

  private static final int    NUM_OF_LOOP_ITERATIONS = 3;
  private static final int    NUM_OF_RETRIES         = 3;
  private static final String CNT_PROP_NAME          = "cnt";

  private Object              parentV1Id;
  private Object              parentV2Id;

  @Override
  protected String getDatabaseName() {
    return "TestAsyncReplMode2Servers";
  }

  protected void dbClient1() {
    OGlobalConfiguration.LOG_CONSOLE_LEVEL.setValue("FINEST");

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getRemoteURL());
      try {
        OrientVertex parentV1 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, parentV1.getRecord().getVersion());
        parentV1Id = parentV1.getId();

        OrientVertex parentV2 = graph.addVertex("vertextype", (String) null);
        graph.commit();
        assertEquals(1, parentV2.getRecord().getVersion());
        parentV2Id = parentV2.getId();

        int countPropValue = 0;
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV1.setProperty(CNT_PROP_NAME, ++countPropValue);
              graph.commit();
              System.out.println("Committing parentV1" + parentV1.getRecord()+"...");
              break;
            } catch (OConcurrentModificationException c) {
              graph.rollback();
              parentV1.reload();
            }
          }

          for (int attempt = 0; attempt < NUM_OF_RETRIES; attempt++) {
            try {
              parentV2.setProperty(CNT_PROP_NAME, countPropValue);
              graph.commit();
              System.out.println("Committing parentV2" + parentV2.getRecord() + "...");
              break;
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

  protected void dbClient2() {
    sleep(1000);

    synchronized (LOCK) {
      OrientBaseGraph graph = new OrientGraph(getLocalURL2());
      try {
        OrientVertex parentV1 = graph.getVertex(parentV1Id);
        assertEquals(1, parentV1.getRecord().getVersion());

        OrientVertex parentV2 = graph.getVertex(parentV2Id);
        assertEquals(1, parentV2.getRecord().getVersion());

        int countPropValue = 0;
        for (int i = 0; i < NUM_OF_LOOP_ITERATIONS; i++) {
          pause();

          if (exceptionInThread != null)
            break;
          sleep(500);

          parentV1.reload();
          parentV2.reload();
          assertEquals("parentV1 (" + parentV1.getRecord() + ")", ++countPropValue, parentV1.getProperty(CNT_PROP_NAME));
          assertEquals("parentV2 (" + parentV2.getRecord() + ")", countPropValue, parentV2.getProperty(CNT_PROP_NAME));
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
